"""
Cliente para o endpoint AJAX interno do Kommo (/ajax/v4/inbox/list).

Usa cookies de sessão web (extraídos via Playwright) para listar
conversas com metadados ricos: lead_id, lead_nome, contact_id,
responsible_user_id, pipeline_id, status_id, chat_source, tags, etc.

Substitui a Talks API como fonte principal de discovery.
"""

import asyncio
import logging
from dataclasses import dataclass, field

import httpx

from app.config import get_settings
from app.services.session_manager import get_cookies, is_configured

logger = logging.getLogger(__name__)

_SOURCE_TO_ORIGIN = {
    "waba": "whatsapp",
    "whatsapp": "whatsapp",
    "telegram": "telegram",
    "instagram": "instagram",
    "facebook": "facebook",
}


@dataclass
class InboxTalk:
    talk_id: int
    chat_id: str
    contact_id: int | None
    contact_name: str | None
    lead_id: int | None
    lead_nome: str | None
    responsible_user_id: int | None
    pipeline_id: int | None
    status_id: int | None
    chat_source: str | None
    origin: str | None
    is_read: bool
    status: str
    last_message_text: str | None
    last_message_at: int | None
    last_message_author: str | None
    created_at: int
    updated_at: int
    tags: list[dict] = field(default_factory=list)


def _parse_talk(t: dict) -> InboxTalk | None:
    chat_id = t.get("chat_id", "")
    if not chat_id:
        return None

    entity = t.get("entity") or {}
    contact = t.get("contact") or {}
    last_msg = t.get("last_message") or {}
    chat_source = t.get("chat_source")

    lead_id = entity.get("id") if entity.get("type") == "leads" else None

    return InboxTalk(
        talk_id=t.get("id", 0),
        chat_id=chat_id,
        contact_id=t.get("contact_id") or contact.get("id"),
        contact_name=contact.get("name"),
        lead_id=lead_id,
        lead_nome=entity.get("title"),
        responsible_user_id=entity.get("main_user_id"),
        pipeline_id=entity.get("pipeline_id"),
        status_id=entity.get("status_id"),
        chat_source=chat_source,
        origin=_SOURCE_TO_ORIGIN.get((chat_source or "").lower()),
        is_read=t.get("is_read", False),
        status=t.get("status", ""),
        last_message_text=last_msg.get("text"),
        last_message_at=last_msg.get("last_message_at"),
        last_message_author=last_msg.get("author"),
        created_at=t.get("created_at", 0),
        updated_at=t.get("updated_at", 0),
        tags=entity.get("tags") or [],
    )


async def list_inbox_talks(
    max_pages: int = 20,
    known_chat_ids: set[str] | None = None,
    known_last_message_at: dict[str, int] | None = None,
) -> list[InboxTalk] | None:
    """
    Lista conversas do inbox via AJAX endpoint interno do Kommo.

    Retorna None se a sessão estiver inválida (precisa refresh).
    Retorna lista vazia se não houver conversas.

    Early-stop inteligente: para após 2 páginas consecutivas sem
    chats novos E sem chats com atividade mais recente que a armazenada.
    Isso garante que chats atualizados recentemente (ex: consultor respondeu)
    sejam sempre capturados.
    """
    if not is_configured():
        logger.debug("Inbox: session cookies nao configurados")
        return None

    settings = get_settings()
    cookies = get_cookies()
    talks: list[InboxTalk] = []

    first_url = (
        f"{settings.kommo_base_url}/ajax/v4/inbox/list"
        "?limit=50&order%5Bsort_by%5D=last_message_at&order%5Bsort_type%5D=desc"
    )
    next_url: str | None = first_url
    consecutive_stale_pages = 0

    async with httpx.AsyncClient(
        cookies=cookies,
        headers={
            "x-requested-with": "XMLHttpRequest",
            "Accept": "*/*",
        },
        timeout=30.0,
        follow_redirects=False,
    ) as client:
        for page in range(max_pages):
            if not next_url:
                break

            try:
                resp = await client.get(next_url)
            except httpx.RequestError as exc:
                logger.warning("Inbox: erro de rede na página %d: %s", page + 1, exc)
                break

            if resp.status_code in (401, 403):
                logger.warning("Inbox: sessão expirada (HTTP %d)", resp.status_code)
                return None

            if resp.status_code == 302:
                logger.warning("Inbox: redirecionado (sessão expirada)")
                return None

            if resp.status_code != 200:
                logger.warning("Inbox: HTTP %d na página %d", resp.status_code, page + 1)
                break

            try:
                data = resp.json()
            except Exception:
                logger.warning("Inbox: resposta não-JSON na página %d", page + 1)
                break

            items = data.get("_embedded", {}).get("talks", [])
            if not items:
                break

            active_on_page = 0
            for t in items:
                talk = _parse_talk(t)
                if talk:
                    talks.append(talk)
                    is_new = known_chat_ids and talk.chat_id not in known_chat_ids
                    is_updated = (
                        known_last_message_at is not None
                        and talk.chat_id in known_last_message_at
                        and talk.last_message_at is not None
                        and talk.last_message_at > known_last_message_at[talk.chat_id]
                    )
                    if is_new or is_updated:
                        active_on_page += 1

            if known_chat_ids is not None:
                if active_on_page == 0:
                    consecutive_stale_pages += 1
                    if consecutive_stale_pages >= 2:
                        logger.info(
                            "Inbox early-stop: 2 páginas sem atividade nova (página %d, total=%d)",
                            page + 1, len(talks),
                        )
                        break
                else:
                    consecutive_stale_pages = 0

            next_link = data.get("_links", {}).get("next", {}).get("href")
            if not next_link or len(items) < 50:
                next_url = None
            else:
                next_url = next_link

            if next_url:
                await asyncio.sleep(0.3)

    logger.info("Inbox: %d talks encontradas (%d páginas)", len(talks), min(page + 1, max_pages))
    return talks
