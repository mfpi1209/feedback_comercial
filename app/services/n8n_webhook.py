"""
Envia payloads ao webhook do n8n via HTTP POST (fire-and-forget).
"""

import asyncio
import logging

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

_TIMEOUT = 10.0
_MAX_RETRIES = 1


async def send_to_n8n(payload: dict) -> bool:
    """
    POST payload JSON para o webhook do n8n.
    Retorna True se enviou com sucesso, False caso contrário.
    """
    settings = get_settings()
    url = settings.n8n_webhook_url
    if not url:
        return False

    for attempt in range(_MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(url, json=payload)

            if resp.status_code in range(200, 300):
                logger.debug("n8n webhook OK (%d) event=%s", resp.status_code, payload.get("event"))
                return True

            logger.warning(
                "n8n webhook %d: %s (attempt %d)",
                resp.status_code, resp.text[:200], attempt + 1,
            )
        except Exception as exc:
            logger.warning("n8n webhook erro (attempt %d): %s", attempt + 1, exc)

        if attempt < _MAX_RETRIES:
            await asyncio.sleep(1)

    return False


def fire_n8n(payload: dict) -> None:
    """Dispara send_to_n8n em background sem bloquear o caller."""
    try:
        asyncio.create_task(_safe_send(payload))
    except RuntimeError:
        logger.debug("Sem event loop ativo para fire_n8n")


async def _safe_send(payload: dict) -> None:
    try:
        await send_to_n8n(payload)
    except Exception:
        logger.exception("Erro inesperado em fire_n8n")
