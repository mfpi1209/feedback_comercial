"""
Gerenciador de tokens do Amojo.
Armazena token + refresh_token em memória e renova automaticamente quando expira.

O token do amojo é obtido do Local Storage do Kommo no navegador (chave: amojo_token).
Estrutura: { token, refreshToken, expiredAt, user }
"""

import logging
import time
from dataclasses import dataclass, field

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

AMOJO_BASE = "https://amojo.kommo.com"
REFRESH_MARGIN_SECONDS = 300  # renovar 5min antes de expirar


@dataclass
class TokenState:
    token: str = ""
    refresh_token: str = ""
    expired_at: int = 0
    user_id: int = 0
    _refreshing: bool = field(default=False, repr=False)

    @property
    def is_expired(self) -> bool:
        if not self.token:
            return True
        return time.time() >= (self.expired_at - REFRESH_MARGIN_SECONDS)


_state = TokenState()


def init_from_settings():
    """Carrega tokens iniciais do .env."""
    settings = get_settings()
    if settings.kommo_amojo_token:
        _state.token = settings.kommo_amojo_token
    if settings.kommo_amojo_refresh_token:
        _state.refresh_token = settings.kommo_amojo_refresh_token
    if _state.token and not _state.expired_at:
        _state.expired_at = int(time.time()) + 86400 * 2


def get_current_token() -> str:
    """Retorna o token atual (pode estar expirado)."""
    return _state.token


def get_token_state() -> dict:
    """Retorna o estado atual dos tokens (para debug/status)."""
    return {
        "token": _state.token[:8] + "..." if _state.token else "",
        "refresh_token": _state.refresh_token[:8] + "..." if _state.refresh_token else "",
        "expired_at": _state.expired_at,
        "is_expired": _state.is_expired,
        "seconds_until_expiry": max(0, _state.expired_at - int(time.time())),
    }


def update_token(token: str, refresh_token: str = "", expired_at: int = 0):
    """Atualiza o token manualmente (ex: via endpoint admin)."""
    _state.token = token
    if refresh_token:
        _state.refresh_token = refresh_token
    if expired_at:
        _state.expired_at = expired_at
    else:
        _state.expired_at = int(time.time()) + 86400 * 2
    logger.info("Token atualizado manualmente. Expira em %d segundos.", _state.expired_at - int(time.time()))


async def get_valid_token() -> str:
    """
    Retorna um token válido. Se expirado, tenta refresh.
    Se refresh falhar, retorna o token atual (pode dar 401).
    """
    if not _state.is_expired:
        return _state.token

    if _state.refresh_token:
        refreshed = await _try_refresh()
        if refreshed:
            return _state.token

    logger.warning("Token expirado e sem refresh disponível. Use PUT /api/kommo/token para atualizar.")
    return _state.token


async def _try_refresh() -> bool:
    """
    Tenta renovar o token usando o refresh_token.
    Testa múltiplos endpoints conhecidos do amojo.
    """
    if _state._refreshing:
        return False
    _state._refreshing = True

    try:
        endpoints = [
            ("POST", f"{AMOJO_BASE}/v2/tokens", {
                "token": _state.token,
                "refresh_token": _state.refresh_token,
            }),
            ("POST", f"{AMOJO_BASE}/v1/tokens/refresh", {
                "refresh_token": _state.refresh_token,
            }),
            ("POST", f"{AMOJO_BASE}/oauth2/token", {
                "grant_type": "refresh_token",
                "refresh_token": _state.refresh_token,
            }),
        ]

        async with httpx.AsyncClient(timeout=15.0) as client:
            for method, url, payload in endpoints:
                try:
                    resp = await client.request(
                        method, url,
                        json=payload,
                        headers={
                            "Content-Type": "application/json",
                            "x-auth-token": _state.token,
                        },
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        new_token = data.get("token") or data.get("access_token")
                        if new_token:
                            _state.token = new_token
                            _state.refresh_token = data.get("refresh_token", data.get("refreshToken", _state.refresh_token))
                            _state.expired_at = data.get("expired_at", data.get("expiredAt", int(time.time()) + 86400 * 2))
                            logger.info("Token renovado via %s. Novo expiry: %d", url, _state.expired_at)
                            return True
                    logger.debug("Refresh via %s retornou %d", url, resp.status_code)
                except Exception:
                    logger.debug("Refresh via %s falhou", url, exc_info=True)

        logger.warning("Nenhum endpoint de refresh funcionou. Token pode estar expirado.")
        return False
    finally:
        _state._refreshing = False
