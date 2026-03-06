"""
Monitor ao vivo de mensagens do Kommo.
Faz polling dos chats registrados a cada N segundos,
detecta mensagens novas e grava no banco automaticamente.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session, dialect_insert
from app.models.message import KommoMessage
from app.models.monitored_chat import MonitoredChat
from app.services.kommo_chats import fetch_chat_history
from app.services.rate_limiter import get_usage
from app.services.token_manager import get_current_token
from app.services.n8n_dispatcher import get_dispatcher

logger = logging.getLogger(__name__)


def _build_message_payload(msg, chat) -> dict:
    """Monta o payload da mensagem para Supabase e n8n."""
    direction = "inbound" if msg.sender_type == "contact" else "outbound"
    consultor = msg.sender_name if msg.sender_type == "user" else None
    sent_at_iso = msg.sent_at.isoformat() if msg.sent_at else None

    return {
        "message_uid": msg.uid,
        "chat_id": msg.chat_id or chat.chat_id,
        "contact_id": chat.contact_id,
        "lead_id": chat.lead_id or 0,
        "lead_nome": chat.label,
        "direction": direction,
        "sender_type": msg.sender_type,
        "sender_name": msg.sender_name,
        "message_text": msg.text,
        "message_type": msg.message_type,
        "media_url": msg.media_url,
        "sent_at": sent_at_iso,
        "consultor_responsavel": consultor,
        "origin": msg.sender_origin,
    }


def _dispatch_message(payload: dict) -> None:
    """Enfileira mensagem no dispatcher centralizado para o n8n."""
    get_dispatcher().enqueue({"event": "new_message", **payload})


MIN_CYCLE_SECONDS = 5
CONCURRENT_POLLS = 3

TIER_HOT_HOURS = 2
TIER_WARM_HOURS = 24
WARM_EVERY_N = 20
COLD_EVERY_N = 100
MAX_WARM_PER_CYCLE = 50
MAX_COLD_PER_CYCLE = 30

_cycle_count = 0
_semaphore: asyncio.Semaphore | None = None


async def run_live_monitor():
    """Loop principal do monitor com polling paralelo por prioridade."""
    global _cycle_count, _semaphore
    _semaphore = asyncio.Semaphore(CONCURRENT_POLLS)
    logger.info(
        "Monitor ao vivo iniciado (hot=%dh, parallel=%d, warm_cada=%d, cold_cada=%d)",
        TIER_HOT_HOURS, CONCURRENT_POLLS, WARM_EVERY_N, COLD_EVERY_N,
    )

    while True:
        try:
            if not get_current_token():
                logger.debug("Monitor: sem amojo token, aguardando...")
                await asyncio.sleep(MIN_CYCLE_SECONDS)
                continue

            _cycle_count += 1
            include_warm = (_cycle_count % WARM_EVERY_N == 0)
            include_cold = (_cycle_count % COLD_EVERY_N == 0)

            async with async_session() as db:
                chats = await _get_prioritized_chats(db, include_warm, include_cold)

            tier_label = "hot"
            if include_cold:
                tier_label = "hot+warm+cold"
            elif include_warm:
                tier_label = "hot+warm"

            if chats:
                total_new = await _poll_all_parallel(chats)

                logger.info(
                    "Monitor ciclo #%d [%s]: %d chats, %d novas msgs | %s",
                    _cycle_count, tier_label, len(chats), total_new, get_usage(),
                )

        except Exception:
            logger.exception("Erro no loop do monitor")

        await asyncio.sleep(MIN_CYCLE_SECONDS)


async def _poll_all_parallel(chats: list) -> int:
    """Faz polling de todos os chats em paralelo, limitado pelo semaforo."""
    results = await asyncio.gather(
        *[_poll_chat_safe(chat) for chat in chats],
        return_exceptions=True,
    )
    return sum(r for r in results if isinstance(r, int))


async def _poll_chat_safe(chat) -> int:
    """Poll com semaforo para limitar concorrencia."""
    async with _semaphore:
        try:
            return await _poll_chat(chat)
        except Exception:
            logger.exception("Erro ao monitorar chat %s", chat.chat_id)
            return 0


async def _get_prioritized_chats(
    db: AsyncSession,
    include_warm: bool,
    include_cold: bool,
) -> list[MonitoredChat]:
    """
    Polling por prioridade:
    - HOT (atividade nas ultimas TIER_HOT_HOURS): sempre
    - WARM (atividade nas ultimas TIER_WARM_HOURS): a cada WARM_EVERY_N ciclos
    - COLD (sem atividade / muito antigo): a cada COLD_EVERY_N ciclos
    """
    now = datetime.now(timezone.utc)
    hot_cutoff = now - timedelta(hours=TIER_HOT_HOURS)

    hot_q = await db.execute(
        select(MonitoredChat)
        .where(
            MonitoredChat.active == True,
            MonitoredChat.last_message_at >= hot_cutoff,
        )
        .order_by(MonitoredChat.last_message_at.desc())
    )
    hot = list(hot_q.scalars().all())

    if not include_warm and not include_cold:
        return hot

    warm_cutoff = now - timedelta(hours=TIER_WARM_HOURS)

    if include_warm or include_cold:
        warm_q = await db.execute(
            select(MonitoredChat)
            .where(
                MonitoredChat.active == True,
                MonitoredChat.last_message_at < hot_cutoff,
                MonitoredChat.last_message_at >= warm_cutoff,
            )
            .order_by(MonitoredChat.last_message_at.desc())
            .limit(MAX_WARM_PER_CYCLE)
        )
        warm = list(warm_q.scalars().all())
    else:
        warm = []

    if include_cold:
        cold_q = await db.execute(
            select(MonitoredChat)
            .where(
                MonitoredChat.active == True,
                (MonitoredChat.last_message_at < warm_cutoff)
                | (MonitoredChat.last_message_at.is_(None)),
            )
            .order_by(MonitoredChat.last_message_at.desc().nulls_last())
            .limit(MAX_COLD_PER_CYCLE)
        )
        cold = list(cold_q.scalars().all())
    else:
        cold = []

    total = hot + warm + cold
    if include_warm or include_cold:
        logger.info(
            "Chats selecionados: %d hot + %d warm + %d cold = %d total",
            len(hot), len(warm), len(cold), len(total),
        )
    return total


async def _poll_chat(chat: MonitoredChat) -> int:
    """
    Verifica um chat por mensagens novas.
    Busca a página mais recente, coleta as novas, e despacha em ordem cronológica.
    """
    messages = await fetch_chat_history(chat.chat_id, limit=50, offset=0)
    if not messages:
        return 0

    new_msgs = []
    for msg in messages:
        if not msg.uid:
            continue
        if chat.last_message_uid and msg.uid == chat.last_message_uid:
            break
        new_msgs.append(msg)

    if not new_msgs:
        return 0

    latest_uid = new_msgs[0].uid
    latest_at = new_msgs[0].sent_at

    new_msgs.reverse()

    inserted = 0
    async with async_session() as db:
        for msg in new_msgs:
            stmt = (
                dialect_insert(KommoMessage)
                .values(
                    lead_id=chat.lead_id or 0,
                    contact_id=chat.contact_id,
                    talk_id=str(msg.dialog_id or ""),
                    chat_id=msg.chat_id or chat.chat_id,
                    sender_name=msg.sender_name,
                    sender_phone=msg.sender_phone,
                    sender_type=msg.sender_type,
                    message_text=msg.text,
                    message_type=msg.message_type,
                    media_url=msg.media_url,
                    sent_at=msg.sent_at,
                    origin=msg.sender_origin,
                    synced_at=datetime.now(timezone.utc),
                    message_uid=msg.uid,
                )
                .on_conflict_do_nothing(index_elements=["message_uid"])
            )
            result = await db.execute(stmt)
            if result.rowcount and result.rowcount > 0:
                inserted += 1
                _dispatch_message(_build_message_payload(msg, chat))

        if inserted > 0:
            await db.execute(
                update(MonitoredChat)
                .where(MonitoredChat.id == chat.id)
                .values(
                    last_message_uid=latest_uid,
                    last_message_at=latest_at,
                )
            )

        await db.commit()

    if inserted > 0:
        logger.info(
            "Chat %s (%s): %d novas mensagens",
            chat.chat_id[:12], chat.label or "sem label", inserted,
        )

    return inserted


async def add_chat_to_monitor(
    chat_id: str,
    label: str | None = None,
    lead_id: int | None = None,
    contact_id: int | None = None,
    do_initial_sync: bool = True,
) -> dict:
    """
    Registra um chat para monitoramento contínuo.
    Opcionalmente faz sync inicial das mensagens existentes.
    """
    async with async_session() as db:
        existing = await db.execute(
            select(MonitoredChat).where(MonitoredChat.chat_id == chat_id)
        )
        chat = existing.scalar_one_or_none()

        if chat:
            if not chat.active:
                chat.active = True
                if label:
                    chat.label = label
                if lead_id is not None:
                    chat.lead_id = lead_id
                await db.commit()
                return {"status": "reactivated", "chat_id": chat_id}
            return {"status": "already_monitored", "chat_id": chat_id}

        new_chat = MonitoredChat(
            chat_id=chat_id,
            label=label,
            lead_id=lead_id,
            contact_id=contact_id,
            active=True,
        )
        db.add(new_chat)
        await db.commit()

    if do_initial_sync:
        messages = await fetch_chat_history(chat_id, limit=50)
        if messages:
            count = 0
            async with async_session() as db:
                for msg in messages:
                    if not msg.uid:
                        continue
                    stmt = (
                        dialect_insert(KommoMessage)
                        .values(
                            lead_id=lead_id or 0,
                            contact_id=contact_id,
                            talk_id=str(msg.dialog_id or ""),
                            chat_id=msg.chat_id or chat_id,
                            sender_name=msg.sender_name,
                            sender_phone=msg.sender_phone,
                            sender_type=msg.sender_type,
                            message_text=msg.text,
                            message_type=msg.message_type,
                            media_url=msg.media_url,
                            sent_at=msg.sent_at,
                            origin=msg.sender_origin,
                            synced_at=datetime.now(timezone.utc),
                            message_uid=msg.uid,
                        )
                        .on_conflict_do_nothing(index_elements=["message_uid"])
                    )
                    result = await db.execute(stmt)
                    if result.rowcount and result.rowcount > 0:
                        count += 1
                        direction = "inbound" if msg.sender_type == "contact" else "outbound"
                        consultor = msg.sender_name if msg.sender_type == "user" else None
                        payload = {
                            "message_uid": msg.uid,
                            "chat_id": msg.chat_id or chat_id,
                            "contact_id": contact_id,
                            "lead_id": lead_id or 0,
                            "lead_nome": label,
                            "direction": direction,
                            "sender_type": msg.sender_type,
                            "sender_name": msg.sender_name,
                            "message_text": msg.text,
                            "message_type": msg.message_type,
                            "media_url": msg.media_url,
                            "sent_at": msg.sent_at.isoformat() if msg.sent_at else None,
                            "consultor_responsavel": consultor,
                            "origin": msg.sender_origin,
                        }
                        _dispatch_message(payload)

                if messages:
                    await db.execute(
                        update(MonitoredChat)
                        .where(MonitoredChat.chat_id == chat_id)
                        .values(
                            last_message_uid=messages[0].uid,
                            last_message_at=messages[0].sent_at,
                        )
                    )
                await db.commit()

            logger.info("Chat %s: sync inicial = %d mensagens", chat_id, count)
            return {"status": "added", "chat_id": chat_id, "initial_sync": count}

    return {"status": "added", "chat_id": chat_id, "initial_sync": 0}
