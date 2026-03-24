"""
Cache de usuarios do Kommo CRM.

Busca /api/v4/users?with=uuid,amojo_id e mantém dois mapeamentos:
  - amojo_uuid (str) -> nome CRM   (usado para resolver author.id da Amojo API)
  - crm_user_id (int) -> nome CRM  (usado para resolver responsible_user_id)

Atualiza a cada 6 horas automaticamente.
"""

import asyncio
import logging
import time

from app.services.kommo_auth import get_bearer_client

logger = logging.getLogger(__name__)

_amojo_map: dict[str, str] = {}
_crm_map: dict[int, str] = {}
_last_refresh: float = 0
_REFRESH_INTERVAL = 6 * 3600
_lock = asyncio.Lock()


async def _fetch_users() -> tuple[dict[str, str], dict[int, str]]:
    """Busca lista de usuarios do Kommo e retorna (amojo_uuid->nome, crm_id->nome)."""
    amojo: dict[str, str] = {}
    crm: dict[int, str] = {}
    try:
        async with get_bearer_client() as client:
            page = 1
            while True:
                resp = await client.get(
                    "/api/v4/users",
                    params={"page": page, "limit": 50, "with": "uuid,amojo_id"},
                )
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
                    amojo_id = u.get("amojo_id", "")
                    if uid and name:
                        crm[uid] = name
                    if amojo_id and name:
                        amojo[amojo_id] = name
                if len(users) < 50:
                    break
                page += 1
        logger.info(
            "Kommo users cache: %d CRM users, %d amojo UUIDs mapeados",
            len(crm), len(amojo),
        )
        for aid, n in amojo.items():
            logger.debug("  amojo %s -> %s", aid[:12], n)
    except Exception:
        logger.exception("Erro ao buscar usuarios do Kommo")
    return amojo, crm


async def _refresh_if_needed() -> None:
    global _amojo_map, _crm_map, _last_refresh
    now = time.time()
    if _amojo_map and (now - _last_refresh) < _REFRESH_INTERVAL:
        return

    async with _lock:
        if _amojo_map and (now - _last_refresh) < _REFRESH_INTERVAL:
            return
        amojo, crm = await _fetch_users()
        if amojo:
            _amojo_map = amojo
            _crm_map = crm
            _last_refresh = now


def get_cached_user_name_by_amojo_id(amojo_uuid: str) -> str | None:
    """Busca nome CRM pelo amojo UUID (author.id da Amojo API)."""
    return _amojo_map.get(amojo_uuid)


def get_cached_user_name(user_id: int) -> str | None:
    """Busca nome CRM pelo user_id inteiro."""
    return _crm_map.get(user_id)


async def ensure_loaded() -> None:
    """Garante que o cache esta carregado (chamar no startup)."""
    await _refresh_if_needed()
