"""
Gerenciador de cookies da sessão web do Kommo.

Armazena cookies extraídos do Playwright após login e os fornece
para requests httpx ao endpoint AJAX interno do Kommo.
Os cookies são persistidos em disco para sobreviver a restarts.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_cookies: dict[str, str] = {}
_COOKIE_PATH = Path(__file__).resolve().parents[2] / "data" / "kommo_session_cookies.json"


def update_cookies(cookies: dict[str, str]) -> None:
    global _cookies
    _cookies = dict(cookies)
    _persist()
    logger.info("Session manager: %d cookies atualizados", len(_cookies))


def update_from_playwright_cookies(pw_cookies: list[dict]) -> None:
    """Converte lista de cookies do Playwright para dict e salva."""
    cookie_dict = {}
    for c in pw_cookies:
        domain = c.get("domain", "")
        if "kommo.com" in domain:
            cookie_dict[c["name"]] = c["value"]
    update_cookies(cookie_dict)


def get_cookies() -> dict[str, str]:
    if not _cookies:
        _load()
    return dict(_cookies)


def is_configured() -> bool:
    cookies = get_cookies()
    return bool(cookies.get("session_id"))


def _persist() -> None:
    try:
        _COOKIE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _COOKIE_PATH.write_text(json.dumps(_cookies, ensure_ascii=False), encoding="utf-8")
    except Exception:
        logger.exception("Session manager: erro ao persistir cookies")


def _load() -> None:
    global _cookies
    if _COOKIE_PATH.exists():
        try:
            _cookies = json.loads(_COOKIE_PATH.read_text(encoding="utf-8"))
            logger.info("Session manager: %d cookies carregados do disco", len(_cookies))
        except Exception:
            logger.exception("Session manager: erro ao carregar cookies")
