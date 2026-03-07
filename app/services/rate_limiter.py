"""
Rate limiter para APIs do Kommo.
Limite global: 400 req/min (Kommo).
Budget do sistema: MAX_RPM configurável (padrão: 100 req/min).
"""

import asyncio
import logging
import time
from collections import deque

logger = logging.getLogger(__name__)

MAX_RPM = 380
_timestamps: deque[float] = deque()
_lock = asyncio.Lock()


async def acquire():
    """
    Aguarda até que seja seguro fazer uma requisição.
    Bloqueia se o limite de RPM foi atingido.
    """
    async with _lock:
        now = time.monotonic()
        window_start = now - 60.0

        while _timestamps and _timestamps[0] < window_start:
            _timestamps.popleft()

        if len(_timestamps) >= MAX_RPM:
            wait_until = _timestamps[0] + 60.0
            wait_seconds = wait_until - now
            if wait_seconds > 0:
                logger.warning(
                    "Rate limit atingido (%d/%d rpm). Aguardando %.1fs...",
                    len(_timestamps), MAX_RPM, wait_seconds,
                )
                await asyncio.sleep(wait_seconds)
                now = time.monotonic()
                window_start = now - 60.0
                while _timestamps and _timestamps[0] < window_start:
                    _timestamps.popleft()

        _timestamps.append(now)


def get_usage() -> dict:
    """Retorna uso atual do rate limiter."""
    now = time.monotonic()
    window_start = now - 60.0
    while _timestamps and _timestamps[0] < window_start:
        _timestamps.popleft()
    return {
        "requests_last_minute": len(_timestamps),
        "max_rpm": MAX_RPM,
        "available": MAX_RPM - len(_timestamps),
    }
