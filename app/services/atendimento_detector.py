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
from app.services.n8n_dispatcher import get_dispatcher
from app.services.supabase_messages import enrich_atendimento_messages

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

    backfilled = await _backfill_contact_ids()
    if backfilled:
        logger.info("Backfill: %d mensagens atualizadas com contact_id", backfilled)

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
                old_count = matched.message_count
                new_msgs = session["count"] > old_count
                if matched.session_end != session["end"]:
                    matched.session_end = session["end"]
                    changed = True
                if new_msgs:
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
                if new_msgs and matched.status == "analisado":
                    matched.status = "aberto"
                    logger.info(
                        "Atendimento #%d reaberto: %d→%d msgs",
                        matched.id, old_count, session["count"],
                    )
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
        stale_q = await db.execute(
            select(Atendimento).where(
                Atendimento.status == "aberto",
                Atendimento.session_end < cutoff,
            )
        )
        stale = list(stale_q.scalars().all())

        if not stale:
            return 0

        for atend in stale:
            atend.status = "fechado"
            atend.updated_at = datetime.now(timezone.utc)

        await db.commit()

    for atend in stale:
        asyncio.create_task(_on_atendimento_closed(atend))

    return len(stale)


async def _on_atendimento_closed(atend: Atendimento) -> None:
    """Enriquece mensagens no Supabase e notifica n8n ao fechar um atendimento."""
    try:
        enriched = await enrich_atendimento_messages(
            atendimento_id=atend.id,
            contact_id=atend.contact_id,
            session_start=atend.session_start,
            session_end=atend.session_end,
        )
        if enriched:
            logger.info(
                "Atendimento #%d: %d mensagens enriquecidas no Supabase",
                atend.id, enriched,
            )
    except Exception:
        logger.exception("Erro ao enriquecer atendimento #%d no Supabase", atend.id)

    try:
        chat_ids = json.loads(atend.chat_ids_json) if atend.chat_ids_json else []
    except (json.JSONDecodeError, TypeError):
        chat_ids = []

    messages_payload = await _fetch_atendimento_messages(
        atend.contact_id, atend.session_start, atend.session_end,
    )

    get_dispatcher().enqueue({
        "event": "atendimento_fechado",
        "atendimento_id": atend.id,
        "contact_id": atend.contact_id,
        "lead_id": atend.lead_id,
        "lead_nome": atend.lead_nome,
        "lead_telefone": atend.lead_telefone,
        "session_start": atend.session_start.isoformat() if atend.session_start else None,
        "session_end": atend.session_end.isoformat() if atend.session_end else None,
        "message_count": atend.message_count,
        "chat_ids": chat_ids,
        "messages": messages_payload,
    })


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


async def _backfill_contact_ids() -> int:
    """
    Propaga contact_id de monitored_chats para kommo_messages
    onde ainda estiver NULL/0.
    """
    total = 0
    async with async_session() as db:
        chat_map_q = await db.execute(
            select(MonitoredChat.chat_id, MonitoredChat.contact_id)
            .where(MonitoredChat.contact_id.isnot(None), MonitoredChat.contact_id != 0)
        )
        chat_map = {row.chat_id: row.contact_id for row in chat_map_q.all()}

        if not chat_map:
            return 0

        for chat_id, contact_id in chat_map.items():
            result = await db.execute(
                update(KommoMessage)
                .where(
                    KommoMessage.chat_id == chat_id,
                    (KommoMessage.contact_id.is_(None)) | (KommoMessage.contact_id == 0),
                )
                .values(contact_id=contact_id)
            )
            total += result.rowcount or 0

        if total:
            await db.commit()

    return total


async def _fetch_atendimento_messages(
    contact_id: int,
    session_start: datetime,
    session_end: datetime,
) -> list[dict]:
    """Busca todas as mensagens de um atendimento no SQLite para enviar ao n8n."""
    async with async_session() as db:
        q = await db.execute(
            select(
                KommoMessage.message_uid,
                KommoMessage.chat_id,
                KommoMessage.sender_name,
                KommoMessage.sender_phone,
                KommoMessage.sender_type,
                KommoMessage.message_text,
                KommoMessage.message_type,
                KommoMessage.media_url,
                KommoMessage.sent_at,
                KommoMessage.origin,
                KommoMessage.lead_id,
            )
            .where(
                KommoMessage.contact_id == contact_id,
                KommoMessage.sent_at >= session_start,
                KommoMessage.sent_at <= session_end,
            )
            .order_by(KommoMessage.sent_at.asc())
        )
        rows = q.all()

    result = []
    for r in rows:
        if r.sender_type == "contact":
            direction = "inbound"
            role = "CLIENTE"
        elif r.sender_type == "user":
            direction = "outbound"
            role = "CONSULTOR"
        else:
            direction = "outbound"
            role = "BOT"

        result.append({
            "message_uid": r.message_uid,
            "chat_id": r.chat_id,
            "sender_name": r.sender_name,
            "sender_type": r.sender_type,
            "role": role,
            "direction": direction,
            "message_text": r.message_text or "",
            "message_type": r.message_type or "text",
            "media_url": r.media_url,
            "sent_at": r.sent_at.isoformat() if r.sent_at else None,
            "origin": r.origin,
            "has_media": bool(r.media_url),
        })

    return result
