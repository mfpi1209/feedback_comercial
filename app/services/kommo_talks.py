"""
Cliente para a Talks API do Kommo (Bearer token).
Lista conversas associadas a leads para obter chat_id, contact_id, origin etc.
"""

import asyncio
import logging
from dataclasses import dataclass

from app.config import get_settings
from app.services.kommo_auth import get_bearer_client
from app.services.rate_limiter import acquire_talks as acquire

logger = logging.getLogger(__name__)


@dataclass
class TalkInfo:
    talk_id: int
    chat_id: str
    contact_id: int | None
    lead_id: int | None
    origin: str | None
    is_in_work: bool
    created_at: int


async def list_talks_for_lead(lead_id: int) -> list[TalkInfo]:
    """Busca todas as conversas (talks) associadas a um lead."""
    if not get_settings().kommo_access_token:
        logger.warning("KOMMO_ACCESS_TOKEN não configurado — Talks API indisponível")
        return []
    talks: list[TalkInfo] = []
    async with get_bearer_client() as client:
        params = {
            "filter[entity_type]": "leads",
            "filter[entity_id]": lead_id,
            "limit": 50,
            "offset": 0,
        }
        while True:
            await acquire()
            resp = await client.get("/api/v4/talks", params=params)
            if resp.status_code == 429:
                logger.warning("Rate limit 429 na Talks API. Aguardando 60s...")
                await asyncio.sleep(60)
                continue
            resp.raise_for_status()
            data = resp.json()

            embedded = data.get("_embedded", {})
            items = embedded.get("talks", [])
            if not items:
                break

            for t in items:
                chat_id = t.get("chat_id", "")
                if not chat_id:
                    continue
                talks.append(
                    TalkInfo(
                        talk_id=t.get("id", 0),
                        chat_id=chat_id,
                        contact_id=t.get("contact_id"),
                        lead_id=lead_id,
                        origin=t.get("origin"),
                        is_in_work=t.get("is_in_work", False),
                        created_at=t.get("created_at", 0),
                    )
                )

            if len(items) < 50:
                break
            params["offset"] += 50

    logger.info("Lead %d: encontradas %d talks com chat_id", lead_id, len(talks))
    return talks


async def list_all_talks(limit: int = 0, known_chat_ids: set[str] | None = None) -> list[TalkInfo]:
    """
    Lista TODAS as conversas ativas (is_in_work) do Kommo.
    Ordenadas por atividade mais recente para pegar novidades primeiro.
    """
    if not get_settings().kommo_access_token:
        logger.warning("KOMMO_ACCESS_TOKEN não configurado — Talks API indisponível")
        return []

    all_talks: list[TalkInfo] = []

    for is_in_work_filter in (True, False):
        talks_batch = await _fetch_talks_page(
            is_in_work=is_in_work_filter,
            limit=limit,
            known_chat_ids=known_chat_ids,
            skip_early_stop=(is_in_work_filter is True),
        )
        all_talks.extend(talks_batch)

        if is_in_work_filter is True:
            logger.info("Talks ativas (is_in_work=true): %d", len(talks_batch))
        else:
            logger.info("Talks recentes inativas: %d", len(talks_batch))

    logger.info("Total de talks listadas: %d", len(all_talks))
    return all_talks


async def _fetch_talks_page(
    is_in_work: bool,
    limit: int = 0,
    known_chat_ids: set[str] | None = None,
    skip_early_stop: bool = False,
) -> list[TalkInfo]:
    """Busca paginas da Talks API com filtro is_in_work."""
    talks: list[TalkInfo] = []
    consecutive_known_pages = 0
    async with get_bearer_client() as client:
        params: dict = {
            "limit": 50,
            "offset": 0,
            "order[updated_at]": "desc",
            "filter[is_in_work]": "true" if is_in_work else "false",
        }
        while True:
            await acquire()
            resp = await client.get("/api/v4/talks", params=params)
            if resp.status_code == 429:
                logger.warning("Rate limit 429 na Talks API. Aguardando 60s...")
                await asyncio.sleep(60)
                continue
            if resp.status_code == 204:
                break
            resp.raise_for_status()
            data = resp.json()

            items = data.get("_embedded", {}).get("talks", [])
            if not items:
                break

            new_on_this_page = 0
            for t in items:
                chat_id = t.get("chat_id", "")
                if not chat_id:
                    continue

                entity_id = t.get("entity_id")
                entity_type = (t.get("entity_type") or "").lower()
                lead_id = entity_id if entity_type in ("lead", "leads") else None

                talks.append(
                    TalkInfo(
                        talk_id=t.get("id", 0),
                        chat_id=chat_id,
                        contact_id=t.get("contact_id"),
                        lead_id=lead_id,
                        origin=t.get("origin"),
                        is_in_work=t.get("is_in_work", False),
                        created_at=t.get("created_at", 0),
                    )
                )

                if known_chat_ids and chat_id not in known_chat_ids:
                    new_on_this_page += 1

            if not skip_early_stop and known_chat_ids is not None:
                if new_on_this_page == 0:
                    consecutive_known_pages += 1
                    if consecutive_known_pages >= 3:
                        logger.info(
                            "Early stop: 3 paginas sem novos chats (offset=%d, total=%d, is_in_work=%s)",
                            params["offset"], len(talks), is_in_work,
                        )
                        break
                else:
                    consecutive_known_pages = 0

            if len(items) < 50:
                break
            params["offset"] += 50

            if limit > 0 and len(talks) >= limit:
                break

    return talks
