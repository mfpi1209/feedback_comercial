"""
Cache de usuarios do Kommo CRM.

Busca /api/v4/users e mantem mapeamento user_id -> nome em memoria.
Atualiza a cada 6 horas automaticamente.
"""

import asyncio
import logging
import time

from app.services.kommo_auth import get_bearer_client

logger = logging.getLogger(__name__)

_user_map: dict[int, str] = {}
_last_refresh: float = 0
_REFRESH_INTERVAL = 6 * 3600  # 6h
_lock = asyncio.Lock()


async def _fetch_users() -> dict[int, str]:
    """Busca lista de usuarios do Kommo e retorna {user_id: nome}."""
    result: dict[int, str] = {}
    try:
        async with get_bearer_client() as client:
            page = 1
            while True:
                resp = await client.get("/api/v4/users", params={"page": page, "limit": 50})
                if resp.status_code != 200:
                    logger.warning("Kommo users API retornou %d", resp.status_code)
                    break
                data = resp.json()
                users = data.get("_embedded", {}).get("users", [])
                if not users:
                    break
                for u in users:
                    uid = u.get("id")
                    name = u.get("name", "")
                    if uid and name:
                        result[uid] = name
                if len(users) < 50:
                    break
                page += 1
        logger.info("Kommo users cache: %d usuarios carregados", len(result))
    except Exception:
        logger.exception("Erro ao buscar usuarios do Kommo")
    return result


async def get_user_map() -> dict[int, str]:
    """Retorna mapeamento user_id -> nome, atualizando se necessario."""
    global _user_map, _last_refresh
    now = time.time()
    if _user_map and (now - _last_refresh) < _REFRESH_INTERVAL:
        return _user_map

    async with _lock:
        if _user_map and (now - _last_refresh) < _REFRESH_INTERVAL:
            return _user_map
        fresh = await _fetch_users()
        if fresh:
            _user_map = fresh
            _last_refresh = now
    return _user_map


def get_cached_user_name(user_id: int) -> str | None:
    """Busca nome de usuario no cache (sem I/O). Retorna None se nao encontrado."""
    return _user_map.get(user_id)


async def ensure_loaded() -> None:
    """Garante que o cache esta carregado (chamar no startup)."""
    await get_user_map()
