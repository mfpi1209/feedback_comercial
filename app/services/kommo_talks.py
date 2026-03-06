"""
Cliente para a Talks API do Kommo (Bearer token).
Lista conversas associadas a leads para obter chat_id, contact_id, origin etc.
"""

import asyncio
import logging
from dataclasses import dataclass

from app.config import get_settings
from app.services.kommo_auth import get_bearer_client
from app.services.rate_limiter import acquire

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


async def list_all_talks(limit: int = 250) -> list[TalkInfo]:
    """Lista todas as conversas recentes (até `limit`)."""
    if not get_settings().kommo_access_token:
        logger.warning("KOMMO_ACCESS_TOKEN não configurado — Talks API indisponível")
        return []
    talks: list[TalkInfo] = []
    async with get_bearer_client() as client:
        params: dict = {"limit": 50, "offset": 0}
        while len(talks) < limit:
            logger.debug("list_all_talks: aguardando rate limiter (offset=%d)...", params["offset"])
            await acquire()
            logger.debug("list_all_talks: fazendo request (offset=%d)...", params["offset"])
            resp = await client.get("/api/v4/talks", params=params)
            logger.debug("list_all_talks: resposta HTTP %d (offset=%d)", resp.status_code, params["offset"])
            if resp.status_code == 429:
                logger.warning("Rate limit 429 na Talks API. Aguardando 60s...")
                await asyncio.sleep(60)
                continue
            resp.raise_for_status()
            data = resp.json()

            items = data.get("_embedded", {}).get("talks", [])
            if not items:
                break

            if not talks:
                logger.debug("Talks API — primeiro item: %s", list(items[0].keys()) if items else "vazio")

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

            if len(items) < 50:
                break
            params["offset"] += 50

    logger.info("Total de talks listadas: %d", len(talks))
    return talks
