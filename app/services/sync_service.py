"""
Serviço de sincronização incremental de mensagens.
Fluxo: Talks API → Chats API v1 (x-auth-token) → PostgreSQL
"""

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import async_session, dialect_insert
from app.models.message import KommoMessage
from app.services.kommo_chats import ChatMessage, fetch_full_chat_history
from app.services.kommo_talks import TalkInfo, list_all_talks, list_talks_for_lead

logger = logging.getLogger(__name__)


async def sync_messages_for_lead(lead_id: int, db: AsyncSession) -> int:
    """Sincroniza mensagens de todas as conversas de um lead. Retorna total inserido."""
    talks = await list_talks_for_lead(lead_id)
    total = 0
    for talk in talks:
        count = await _sync_chat(talk, db)
        total += count
    return total


async def sync_all_messages() -> int:
    """Sincroniza mensagens de todas as conversas recentes. Roda em background."""
    talks = await list_all_talks()
    total = 0

    async with async_session() as db:
        for talk in talks:
            try:
                count = await _sync_chat(talk, db)
                total += count
            except Exception:
                logger.exception("Erro ao sincronizar chat %s", talk.chat_id)
                continue

    logger.info("Sync completo: %d novas mensagens inseridas", total)
    return total


async def _sync_chat(talk: TalkInfo, db: AsyncSession) -> int:
    """Busca mensagens de um chat e faz upsert no banco."""
    messages = await fetch_full_chat_history(talk.chat_id)
    if not messages:
        return 0

    inserted = 0
    for msg in messages:
        if not msg.uid:
            continue

        origin = msg.sender_origin or talk.origin

        stmt = (
            dialect_insert(KommoMessage)
            .values(
                lead_id=talk.lead_id or 0,
                contact_id=talk.contact_id,
                talk_id=str(talk.talk_id),
                chat_id=talk.chat_id,
                sender_name=msg.sender_name,
                sender_phone=msg.sender_phone,
                sender_type=msg.sender_type,
                message_text=msg.text,
                message_type=msg.message_type,
                media_url=msg.media_url,
                sent_at=msg.sent_at,
                origin=origin,
                synced_at=datetime.now(timezone.utc),
                message_uid=msg.uid,
            )
            .on_conflict_do_nothing(index_elements=["message_uid"])
        )
        result = await db.execute(stmt)
        if result.rowcount and result.rowcount > 0:
            inserted += 1

    await db.commit()
    logger.info("Chat %s: %d novas mensagens (de %d total)", talk.chat_id, inserted, len(messages))
    return inserted


async def run_periodic_sync():
    """Loop de sync periódico para rodar como background task."""
    settings = get_settings()
    while True:
        try:
            logger.info("Iniciando sync periódico...")
            count = await sync_all_messages()
            logger.info("Sync periódico concluído: %d mensagens", count)
        except Exception:
            logger.exception("Erro no sync periódico")
        await asyncio.sleep(settings.sync_interval_seconds)
