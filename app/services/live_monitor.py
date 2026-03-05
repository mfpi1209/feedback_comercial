"""
Monitor ao vivo de mensagens do Kommo.
Faz polling dos chats registrados a cada N segundos,
detecta mensagens novas e grava no banco automaticamente.
"""

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session, dialect_insert
from app.models.message import KommoMessage
from app.models.monitored_chat import MonitoredChat
from app.services.kommo_chats import fetch_chat_history
from app.services.rate_limiter import get_usage
from app.services.token_manager import get_current_token

logger = logging.getLogger(__name__)

MIN_CYCLE_SECONDS = 30
DELAY_BETWEEN_CHATS = 1.0


async def run_live_monitor():
    """Loop principal do monitor. Roda indefinidamente."""
    logger.info("Monitor ao vivo iniciado (min ciclo: %ds, delay entre chats: %.1fs)",
                MIN_CYCLE_SECONDS, DELAY_BETWEEN_CHATS)

    while True:
        try:
            if not get_current_token():
                logger.debug("Monitor: sem amojo token, aguardando...")
                await asyncio.sleep(MIN_CYCLE_SECONDS)
                continue

            async with async_session() as db:
                chats = await _get_active_chats(db)

            if chats:
                total_new = 0
                for i, chat in enumerate(chats):
                    try:
                        count = await _poll_chat(chat)
                        total_new += count
                    except Exception:
                        logger.exception("Erro ao monitorar chat %s", chat.chat_id)

                    if i < len(chats) - 1:
                        await asyncio.sleep(DELAY_BETWEEN_CHATS)

                if total_new > 0:
                    logger.info("Monitor: %d novas mensagens de %d chats | %s",
                                total_new, len(chats), get_usage())

        except Exception:
            logger.exception("Erro no loop do monitor")

        await asyncio.sleep(MIN_CYCLE_SECONDS)


async def _get_active_chats(db: AsyncSession) -> list[MonitoredChat]:
    """Busca todos os chats marcados como ativos para monitoramento."""
    stmt = select(MonitoredChat).where(MonitoredChat.active == True)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def _poll_chat(chat: MonitoredChat) -> int:
    """
    Verifica um chat por mensagens novas.
    Busca a página mais recente e insere apenas as que ainda não existem.
    """
    messages = await fetch_chat_history(chat.chat_id, limit=50, offset=0)
    if not messages:
        return 0

    inserted = 0
    latest_uid = None
    latest_at = None

    async with async_session() as db:
        for msg in messages:
            if not msg.uid:
                continue

            if chat.last_message_uid and msg.uid == chat.last_message_uid:
                break

            stmt = (
                dialect_insert(KommoMessage)
                .values(
                    lead_id=chat.lead_id or 0,
                    contact_id=None,
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

            if latest_uid is None:
                latest_uid = msg.uid
                latest_at = msg.sent_at

        if latest_uid and inserted > 0:
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
                            contact_id=None,
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
