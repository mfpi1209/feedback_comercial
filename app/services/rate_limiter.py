"""
Rate limiters separados para as APIs do Kommo.

- Amojo API (x-auth-token, amojo.kommo.com): mensagens de chat
- Talks API (Bearer token, kommo.com/api/v4): discovery de conversas

Cada API tem seu próprio limite independente.
"""

import asyncio
import logging
import time
from collections import deque

logger = logging.getLogger(__name__)

AMOJO_MAX_RPM = 380
TALKS_MAX_RPM = 40


class _RateLimiter:
    def __init__(self, name: str, max_rpm: int):
        self.name = name
        self.max_rpm = max_rpm
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            window_start = now - 60.0

            while self._timestamps and self._timestamps[0] < window_start:
                self._timestamps.popleft()

            if len(self._timestamps) >= self.max_rpm:
                wait_until = self._timestamps[0] + 60.0
                wait_seconds = wait_until - now
                if wait_seconds > 0:
                    logger.warning(
                        "%s rate limit atingido (%d/%d rpm). Aguardando %.1fs...",
                        self.name, len(self._timestamps), self.max_rpm, wait_seconds,
                    )
                    await asyncio.sleep(wait_seconds)
                    now = time.monotonic()
                    window_start = now - 60.0
                    while self._timestamps and self._timestamps[0] < window_start:
                        self._timestamps.popleft()

            self._timestamps.append(now)

    def get_usage(self) -> dict:
        now = time.monotonic()
        window_start = now - 60.0
        while self._timestamps and self._timestamps[0] < window_start:
            self._timestamps.popleft()
        return {
            "requests_last_minute": len(self._timestamps),
            "max_rpm": self.max_rpm,
            "available": self.max_rpm - len(self._timestamps),
        }


_amojo_limiter = _RateLimiter("Amojo", AMOJO_MAX_RPM)
_talks_limiter = _RateLimiter("Talks", TALKS_MAX_RPM)


async def acquire():
    """Rate limit para Amojo API (mensagens). Retrocompat."""
    await _amojo_limiter.acquire()


async def acquire_talks():
    """Rate limit para Talks API (discovery de conversas)."""
    await _talks_limiter.acquire()


def get_usage() -> dict:
    """Retorna uso combinado dos rate limiters."""
    amojo = _amojo_limiter.get_usage()
    talks = _talks_limiter.get_usage()
    return {
        "amojo": amojo,
        "talks": talks,
        "requests_last_minute": amojo["requests_last_minute"] + talks["requests_last_minute"],
        "max_rpm": amojo["max_rpm"] + talks["max_rpm"],
        "available": amojo["available"] + talks["available"],
    }
