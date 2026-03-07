"""
Descoberta automática de chats ativos no Kommo.
Usa a Talks API (Bearer token) para listar todas as conversas
e registra automaticamente no monitoramento ao vivo.
"""

import asyncio
import logging

from sqlalchemy import select

from app.config import get_settings
from app.database import async_session
from app.models.monitored_chat import MonitoredChat
from app.services.kommo_talks import list_all_talks

logger = logging.getLogger(__name__)

DISCOVERY_INTERVAL_SECONDS = 300


async def run_chat_discovery():
    """
    Loop que descobre novos chats periodicamente via Talks API
    e registra no monitoramento automaticamente.
    """
    settings = get_settings()
    if not settings.kommo_access_token:
        logger.warning("KOMMO_ACCESS_TOKEN não configurado — discovery desabilitado")
        return

    logger.info("Discovery de chats iniciado (intervalo: %ds)", DISCOVERY_INTERVAL_SECONDS)

    while True:
        try:
            logger.info("Discovery: iniciando busca de chats...")
            new_count = await discover_and_register_chats()
            if new_count > 0:
                logger.info("Discovery: %d novos chats registrados para monitoramento", new_count)
            else:
                logger.info("Discovery: nenhum chat novo encontrado")
        except Exception:
            logger.exception("Erro no discovery de chats")

        await asyncio.sleep(DISCOVERY_INTERVAL_SECONDS)


async def discover_and_register_chats() -> int:
    """
    Busca todas as conversas via Talks API e registra as que
    ainda não estão sendo monitoradas. Também atualiza lead_id
    de chats já existentes que ainda não tinham esse dado.
    Retorna qtd de novos chats registrados.
    """
    talks = await list_all_talks()
    if not talks:
        return 0

    async with async_session() as db:
        existing_rows = await db.execute(select(MonitoredChat))
        existing_map: dict[str, MonitoredChat] = {
            row.chat_id: row for row in existing_rows.scalars().all()
        }

        new_count = 0
        updated_count = 0
        for talk in talks:
            label = f"lead:{talk.lead_id}" if talk.lead_id else f"contact:{talk.contact_id}"
            if talk.origin:
                label += f" ({talk.origin})"

            if talk.chat_id in existing_map:
                mon = existing_map[talk.chat_id]
                changed = False
                if talk.lead_id and (not mon.lead_id or mon.lead_id == 0):
                    mon.lead_id = talk.lead_id
                    mon.label = label
                    changed = True
                if talk.contact_id and not mon.contact_id:
                    mon.contact_id = talk.contact_id
                    changed = True
                if changed:
                    updated_count += 1
                continue

            chat = MonitoredChat(
                chat_id=talk.chat_id,
                label=label,
                lead_id=talk.lead_id,
                contact_id=talk.contact_id,
                active=True,
            )
            db.add(chat)
            existing_map[talk.chat_id] = chat
            new_count += 1
            logger.info("Novo chat descoberto: %s — %s", talk.chat_id[:12], label)

        if new_count > 0 or updated_count > 0:
            await db.commit()

        if updated_count > 0:
            logger.info("Discovery: %d chats atualizados com lead_id", updated_count)

    return new_count
