"""
Descoberta automática de chats ativos no Kommo.

Fonte principal: endpoint AJAX /ajax/v4/inbox/list (cookies de sessão)
  - Retorna dados ricos: lead_id, lead_nome, contact_id, responsible_user_id,
    pipeline_id, status_id, chat_source, tags, última mensagem.

Fallback: Talks API (Bearer token)
  - Menos dados, mas funciona sem sessão web.
"""

import asyncio
import logging
import time

from sqlalchemy import select

from app.config import get_settings
from app.database import async_session
from app.models.monitored_chat import MonitoredChat
from app.services.kommo_inbox import InboxTalk, list_inbox_talks
from app.services.kommo_talks import list_all_talks

logger = logging.getLogger(__name__)

DISCOVERY_INTERVAL_SECONDS = 120
_SESSION_REFRESH_COOLDOWN = 300
_last_session_refresh: float = 0


async def run_chat_discovery():
    """
    Loop que descobre novos chats periodicamente.
    Tenta o inbox AJAX primeiro; se a sessão expirar, tenta refresh
    via Playwright e, em último caso, usa a Talks API como fallback.
    """
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
    Busca conversas ativas e registra as novas no monitoramento.
    Atualiza metadados de chats já existentes.
    Retorna qtd de novos chats registrados.
    """
    async with async_session() as db:
        existing_rows = await db.execute(select(MonitoredChat))
        existing_map: dict[str, MonitoredChat] = {
            row.chat_id: row for row in existing_rows.scalars().all()
        }

    known_ids = set(existing_map.keys())

    inbox_talks = await list_inbox_talks(max_pages=10, known_chat_ids=known_ids)

    if inbox_talks is None:
        logger.warning("Discovery: inbox indisponível, tentando refresh de sessão...")
        refreshed = await _try_session_refresh()
        if refreshed:
            inbox_talks = await list_inbox_talks(max_pages=10, known_chat_ids=known_ids)

    if inbox_talks is not None and len(inbox_talks) > 0:
        logger.info("Discovery via INBOX: %d talks encontradas", len(inbox_talks))
        return await _register_inbox_talks(inbox_talks)

    if inbox_talks is not None and len(inbox_talks) == 0:
        logger.info("Discovery via INBOX: nenhuma talk ativa")
        return 0

    logger.warning("Discovery: inbox falhou, usando Talks API como fallback")
    settings = get_settings()
    if not settings.kommo_access_token:
        logger.warning("Discovery: Talks API também indisponível (sem Bearer token)")
        return 0

    return await _discover_via_talks_api(known_ids)


async def _try_session_refresh() -> bool:
    """Tenta renovar a sessão via Playwright (com cooldown de 5 min)."""
    global _last_session_refresh
    now = time.time()
    if now - _last_session_refresh < _SESSION_REFRESH_COOLDOWN:
        logger.debug("Discovery: refresh de sessão em cooldown, pulando")
        return False

    _last_session_refresh = now
    try:
        from app.services.token_renewer import renew_token_once
        success = await renew_token_once()
        if success:
            logger.info("Discovery: sessão renovada com sucesso via Playwright")
        return success
    except Exception:
        logger.exception("Discovery: erro ao renovar sessão")
        return False


async def _register_inbox_talks(talks: list[InboxTalk]) -> int:
    """Registra talks do inbox no monitoramento."""
    async with async_session() as db:
        existing_rows = await db.execute(select(MonitoredChat))
        existing_map = {
            row.chat_id: row for row in existing_rows.scalars().all()
        }

        new_count = 0
        updated_count = 0

        for talk in talks:
            label = talk.lead_nome or talk.contact_name or f"lead:{talk.lead_id}"
            if talk.origin:
                label += f" ({talk.origin})"

            if talk.chat_id in existing_map:
                mon = existing_map[talk.chat_id]
                changed = _update_chat_metadata(mon, talk, label)
                if changed:
                    updated_count += 1
                continue

            chat = MonitoredChat(
                chat_id=talk.chat_id,
                label=label,
                lead_id=talk.lead_id,
                contact_id=talk.contact_id,
                lead_nome=talk.lead_nome,
                contact_name=talk.contact_name,
                responsible_user_id=talk.responsible_user_id,
                pipeline_id=talk.pipeline_id,
                status_id=talk.status_id,
                chat_source=talk.chat_source,
                active=True,
            )
            db.add(chat)
            existing_map[talk.chat_id] = chat
            new_count += 1
            logger.info(
                "Novo chat [INBOX]: %s — %s (lead=%s, contact=%s)",
                talk.chat_id[:12], label, talk.lead_id, talk.contact_id,
            )

        if new_count > 0 or updated_count > 0:
            await db.commit()

        if updated_count > 0:
            logger.info("Discovery: %d chats atualizados com metadados do inbox", updated_count)

    return new_count


def _update_chat_metadata(mon: MonitoredChat, talk: InboxTalk, label: str) -> bool:
    """Atualiza metadados de um chat existente. Retorna True se algo mudou."""
    changed = False

    if talk.lead_id and (not mon.lead_id or mon.lead_id == 0):
        mon.lead_id = talk.lead_id
        changed = True

    if talk.contact_id and not mon.contact_id:
        mon.contact_id = talk.contact_id
        changed = True

    if talk.lead_nome and mon.lead_nome != talk.lead_nome:
        mon.lead_nome = talk.lead_nome
        mon.label = label
        changed = True

    if talk.contact_name and not mon.contact_name:
        mon.contact_name = talk.contact_name
        changed = True

    if talk.responsible_user_id and mon.responsible_user_id != talk.responsible_user_id:
        mon.responsible_user_id = talk.responsible_user_id
        changed = True

    if talk.pipeline_id and mon.pipeline_id != talk.pipeline_id:
        mon.pipeline_id = talk.pipeline_id
        changed = True

    if talk.status_id and mon.status_id != talk.status_id:
        mon.status_id = talk.status_id
        changed = True

    if talk.chat_source and not mon.chat_source:
        mon.chat_source = talk.chat_source
        changed = True

    return changed


async def _discover_via_talks_api(known_ids: set[str]) -> int:
    """Fallback: discovery via Talks API (Bearer token)."""
    talks = await list_all_talks(known_chat_ids=known_ids)
    if not talks:
        return 0

    async with async_session() as db:
        existing_rows = await db.execute(select(MonitoredChat))
        existing_map = {
            row.chat_id: row for row in existing_rows.scalars().all()
        }

        new_count = 0
        for talk in talks:
            label = f"lead:{talk.lead_id}" if talk.lead_id else f"contact:{talk.contact_id}"
            if talk.origin:
                label += f" ({talk.origin})"

            if talk.chat_id in existing_map:
                mon = existing_map[talk.chat_id]
                if talk.lead_id and (not mon.lead_id or mon.lead_id == 0):
                    mon.lead_id = talk.lead_id
                    mon.label = label
                if talk.contact_id and not mon.contact_id:
                    mon.contact_id = talk.contact_id
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
            logger.info(
                "Novo chat [TALKS]: %s — %s",
                talk.chat_id[:12], label,
            )

        if new_count > 0:
            await db.commit()

    return new_count
