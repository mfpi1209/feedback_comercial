"""
Detecta e agrupa mensagens em "atendimentos" por contact_id.

Um atendimento = todas as mensagens de um mesmo contact_id onde o gap
entre mensagens consecutivas nao excede SESSION_GAP_HOURS.
Se o gap for maior, inicia um novo atendimento.

Roda periodicamente em background para manter a tabela atendimentos atualizada.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import distinct, func, select, update

from app.database import async_session
from app.models.atendimento import Atendimento
from app.models.message import KommoMessage
from app.models.monitored_chat import MonitoredChat

logger = logging.getLogger(__name__)

SESSION_GAP_HOURS = 24
DETECTOR_INTERVAL_SECONDS = 300
CLOSE_AFTER_HOURS = 2


async def run_atendimento_detector():
    """Background loop que detecta e atualiza atendimentos."""
    logger.info(
        "Detector de atendimentos iniciado (gap=%dh, intervalo=%ds, fecha_apos=%dh)",
        SESSION_GAP_HOURS, DETECTOR_INTERVAL_SECONDS, CLOSE_AFTER_HOURS,
    )
    while True:
        try:
            created, updated, closed = await detect_atendimentos()
            if created or updated or closed:
                logger.info(
                    "Atendimentos: %d criados, %d atualizados, %d fechados",
                    created, updated, closed,
                )
        except Exception:
            logger.exception("Erro no detector de atendimentos")
        await asyncio.sleep(DETECTOR_INTERVAL_SECONDS)


async def detect_atendimentos() -> tuple[int, int, int]:
    """
    Varre mensagens agrupadas por contact_id, detecta sessoes e
    cria/atualiza registros na tabela atendimentos.
    Retorna (criados, atualizados, fechados).
    """
    created = 0
    updated = 0
    closed = 0

    async with async_session() as db:
        contact_ids_q = await db.execute(
            select(distinct(KommoMessage.contact_id))
            .where(KommoMessage.contact_id.isnot(None), KommoMessage.contact_id != 0)
        )
        contact_ids = [row[0] for row in contact_ids_q.all()]

    for cid in contact_ids:
        try:
            c, u = await _process_contact(cid)
            created += c
            updated += u
        except Exception:
            logger.exception("Erro processando contact_id=%s", cid)

    closed = await _close_stale_atendimentos()
    return created, updated, closed


async def _process_contact(contact_id: int) -> tuple[int, int]:
    """Detecta sessoes para um contact_id e sincroniza com a tabela."""
    created = 0
    updated = 0

    async with async_session() as db:
        msgs_q = await db.execute(
            select(
                KommoMessage.chat_id,
                KommoMessage.sent_at,
                KommoMessage.sender_name,
                KommoMessage.sender_phone,
                KommoMessage.sender_type,
            )
            .where(KommoMessage.contact_id == contact_id)
            .order_by(KommoMessage.sent_at.asc())
        )
        messages = msgs_q.all()

    if not messages:
        return 0, 0

    sessions = _split_into_sessions(messages)

    lead_id = await get_lead_id_for_contact(contact_id)
    _, lead_nome, lead_telefone = _extract_lead_info(contact_id, messages)

    async with async_session() as db:
        existing_q = await db.execute(
            select(Atendimento)
            .where(Atendimento.contact_id == contact_id)
            .order_by(Atendimento.session_start.asc())
        )
        existing = list(existing_q.scalars().all())

        existing_by_start = {a.session_start: a for a in existing}

        for session in sessions:
            chat_ids_json = json.dumps(session["chat_ids"])
            matched = _find_matching_atendimento(session, existing)

            if matched:
                changed = False
                if matched.session_end != session["end"]:
                    matched.session_end = session["end"]
                    changed = True
                if matched.message_count != session["count"]:
                    matched.message_count = session["count"]
                    changed = True
                if matched.chat_ids_json != chat_ids_json:
                    matched.chat_ids_json = chat_ids_json
                    changed = True
                if lead_id and not matched.lead_id:
                    matched.lead_id = lead_id
                    changed = True
                if lead_nome and not matched.lead_nome:
                    matched.lead_nome = lead_nome
                    changed = True
                if lead_telefone and not matched.lead_telefone:
                    matched.lead_telefone = lead_telefone
                    changed = True
                if changed:
                    matched.updated_at = datetime.now(timezone.utc)
                    updated += 1
            else:
                atend = Atendimento(
                    contact_id=contact_id,
                    lead_id=lead_id,
                    lead_nome=lead_nome,
                    lead_telefone=lead_telefone,
                    chat_ids_json=chat_ids_json,
                    session_start=session["start"],
                    session_end=session["end"],
                    message_count=session["count"],
                    status="aberto",
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
                db.add(atend)
                existing.append(atend)
                created += 1

        if created or updated:
            await db.commit()

    return created, updated


def _split_into_sessions(messages: list) -> list[dict]:
    """
    Divide mensagens ordenadas cronologicamente em sessoes
    baseadas no gap maximo entre mensagens consecutivas.
    """
    gap = timedelta(hours=SESSION_GAP_HOURS)
    sessions: list[dict] = []
    current_chat_ids: set[str] = set()
    current_start = messages[0].sent_at
    current_end = messages[0].sent_at
    current_count = 0

    for msg in messages:
        if current_count > 0 and (msg.sent_at - current_end) > gap:
            sessions.append({
                "start": current_start,
                "end": current_end,
                "count": current_count,
                "chat_ids": sorted(current_chat_ids),
            })
            current_chat_ids = set()
            current_start = msg.sent_at
            current_end = msg.sent_at
            current_count = 0

        current_chat_ids.add(msg.chat_id)
        current_end = msg.sent_at
        current_count += 1

    if current_count > 0:
        sessions.append({
            "start": current_start,
            "end": current_end,
            "count": current_count,
            "chat_ids": sorted(current_chat_ids),
        })

    return sessions


def _find_matching_atendimento(session: dict, existing: list[Atendimento]) -> Atendimento | None:
    """Encontra atendimento existente que corresponde a esta sessao (overlap temporal)."""
    for a in existing:
        if a.status == "analisado":
            if a.session_start == session["start"]:
                return a
            continue
        if a.session_start <= session["end"] and a.session_end >= session["start"]:
            return a
        if a.session_start == session["start"]:
            return a
    return None


def _extract_lead_info(contact_id: int, messages: list) -> tuple[None, str | None, str | None]:
    """Extrai nome e telefone das mensagens do contato."""
    lead_nome = None
    lead_telefone = None
    for msg in messages:
        if msg.sender_type == "contact":
            if msg.sender_name and not lead_nome:
                lead_nome = msg.sender_name
            if msg.sender_phone and not lead_telefone:
                lead_telefone = msg.sender_phone
            if lead_nome and lead_telefone:
                break
    return None, lead_nome, lead_telefone


async def _close_stale_atendimentos() -> int:
    """Marca atendimentos como 'fechado' se nao ha mensagens novas ha CLOSE_AFTER_HOURS."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=CLOSE_AFTER_HOURS)
    async with async_session() as db:
        result = await db.execute(
            update(Atendimento)
            .where(
                Atendimento.status == "aberto",
                Atendimento.session_end < cutoff,
            )
            .values(status="fechado", updated_at=datetime.now(timezone.utc))
        )
        await db.commit()
        return result.rowcount or 0


async def get_lead_id_for_contact(contact_id: int) -> int | None:
    """Busca lead_id pelo contact_id na tabela monitored_chats."""
    async with async_session() as db:
        result = await db.execute(
            select(MonitoredChat.lead_id)
            .where(
                MonitoredChat.contact_id == contact_id,
                MonitoredChat.lead_id.isnot(None),
                MonitoredChat.lead_id != 0,
            )
            .limit(1)
        )
        return result.scalar()
