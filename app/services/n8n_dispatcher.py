"""
Dispatcher centralizado para o n8n.

Todas as mensagens e eventos passam por aqui antes de ir ao webhook.
Organiza, enfileira, controla concorrencia e loga cada operacao.
"""

import asyncio
import logging
from datetime import datetime, timezone

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

_TIMEOUT = 15.0
_MAX_RETRIES = 2
_RETRY_DELAY = 3.0


class N8nDispatcher:

    def __init__(self, max_workers: int = 2, send_interval: float = 0.5):
        self._queue: asyncio.Queue[dict] = asyncio.Queue()
        self._max_workers = max_workers
        self._send_interval = send_interval
        self._sent = 0
        self._failed = 0
        self._total_enqueued = 0
        self._recent: list[dict] = []
        self._running = False

    def enqueue(self, payload: dict) -> None:
        payload["_queued_at"] = datetime.now(timezone.utc).isoformat()
        self._queue.put_nowait(payload)
        self._total_enqueued += 1

        event = payload.get("event", "?")
        uid = (payload.get("message_uid") or "")[:16]
        lead = payload.get("lead_nome") or payload.get("contact_id") or ""
        logger.info(
            "FILA +1 [%s] uid=%s lead=%s | pendentes=%d",
            event, uid, lead, self._queue.qsize(),
        )

    async def run(self) -> None:
        self._running = True
        url = get_settings().n8n_webhook_url
        logger.info(
            "N8n dispatcher iniciado (%d workers, url=%s)",
            self._max_workers, url or "NAO CONFIGURADA",
        )
        if not url:
            logger.error("N8N_WEBHOOK_URL nao configurada — dispatcher inativo")
            return

        workers = [
            asyncio.create_task(self._worker(i, url))
            for i in range(self._max_workers)
        ]
        await asyncio.gather(*workers)

    async def _worker(self, wid: int, url: str) -> None:
        while self._running:
            try:
                payload = await asyncio.wait_for(self._queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue

            await asyncio.sleep(self._send_interval)

            event = payload.get("event", "?")
            uid = (payload.get("message_uid") or "")[:16]
            lead = payload.get("lead_nome") or ""
            msg_type = payload.get("message_type") or "text"
            queued_at = payload.pop("_queued_at", "")

            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                        resp = await client.post(url, json=payload)

                    status = resp.status_code
                    body_preview = resp.text[:300] if resp.text else ""

                    if 200 <= status < 300:
                        self._sent += 1
                        self._add_recent(event, uid, lead, msg_type, status, "ok")
                        logger.info(
                            "N8N OK [%s] uid=%s tipo=%s lead=%s | "
                            "status=%d body=%s | pendentes=%d",
                            event, uid, msg_type, lead,
                            status, body_preview[:80], self._queue.qsize(),
                        )
                        break
                    else:
                        logger.warning(
                            "N8N HTTP %d [%s] uid=%s | body=%s | tentativa %d/%d",
                            status, event, uid, body_preview, attempt, _MAX_RETRIES,
                        )
                        if attempt < _MAX_RETRIES:
                            await asyncio.sleep(_RETRY_DELAY)

                except Exception as exc:
                    logger.warning(
                        "N8N ERRO [%s] uid=%s | %s | tentativa %d/%d",
                        event, uid, exc, attempt, _MAX_RETRIES,
                    )
                    if attempt < _MAX_RETRIES:
                        await asyncio.sleep(_RETRY_DELAY)
            else:
                self._failed += 1
                self._add_recent(event, uid, lead, msg_type, 0, "failed")
                logger.error(
                    "N8N FALHA DEFINITIVA [%s] uid=%s lead=%s | esgotou %d tentativas",
                    event, uid, lead, _MAX_RETRIES,
                )

            self._queue.task_done()

    def _add_recent(self, event, uid, lead, msg_type, status, result):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "uid": uid,
            "lead": lead,
            "type": msg_type,
            "http": status,
            "result": result,
        }
        self._recent.append(entry)
        if len(self._recent) > 50:
            self._recent = self._recent[-50:]

    def get_stats(self) -> dict:
        return {
            "pending": self._queue.qsize(),
            "total_enqueued": self._total_enqueued,
            "sent_ok": self._sent,
            "failed": self._failed,
            "webhook_url": get_settings().n8n_webhook_url or "",
            "recent": list(reversed(self._recent[-20:])),
        }


_dispatcher: N8nDispatcher | None = None


def get_dispatcher() -> N8nDispatcher:
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = N8nDispatcher()
    return _dispatcher
