"""
Autenticação para as APIs do Kommo:
- Bearer token para a API principal (Talks, Leads, Notes)
- x-auth-token para a Chats API interna (amojo v1)
- HMAC-SHA1 para a Chats API documentada (amojo v2, legacy/fallback)
"""

import hashlib
import hmac
from datetime import datetime, timezone
from email.utils import formatdate
from time import mktime

import httpx

from app.config import get_settings


def _bearer_headers() -> dict[str, str]:
    settings = get_settings()
    return {
        "Authorization": f"Bearer {settings.kommo_access_token}",
        "Content-Type": "application/json",
    }


def _amojo_v1_headers(token: str) -> dict[str, str]:
    return {
        "x-auth-token": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _rfc2822_now() -> str:
    now = datetime.now(timezone.utc)
    stamp = mktime(now.timetuple())
    return formatdate(timeval=stamp, localtime=False, usegmt=True)


def _compute_hmac_signature(
    method: str,
    content_md5: str,
    content_type: str,
    date: str,
    path: str,
    secret: str,
) -> str:
    """HMAC-SHA1 conforme documentação amojo do Kommo."""
    check_str = "\n".join([method.upper(), content_md5, content_type, date, path])
    sig = hmac.new(secret.encode("utf-8"), check_str.encode("utf-8"), hashlib.sha1)
    return sig.hexdigest()


def amojo_headers(method: str, path: str, body: bytes = b"") -> dict[str, str]:
    """Gera headers HMAC-SHA1 para a Chats API v2 (legacy/fallback)."""
    settings = get_settings()
    content_type = "application/json"
    content_md5 = hashlib.md5(body).hexdigest() if body else hashlib.md5(b"").hexdigest()
    date = _rfc2822_now()

    signature = _compute_hmac_signature(
        method=method,
        content_md5=content_md5,
        content_type=content_type,
        date=date,
        path=path,
        secret=settings.kommo_chat_channel_secret,
    )

    return {
        "Date": date,
        "Content-Type": content_type,
        "Content-MD5": content_md5,
        "X-Signature": signature,
    }


def get_bearer_client() -> httpx.AsyncClient:
    settings = get_settings()
    return httpx.AsyncClient(
        base_url=settings.kommo_base_url,
        headers=_bearer_headers(),
        timeout=60.0,
    )


async def get_amojo_v1_client() -> httpx.AsyncClient:
    """Client para a API interna do amojo (v1) usando x-auth-token com auto-refresh."""
    from app.services.token_manager import get_valid_token
    token = await get_valid_token()
    return httpx.AsyncClient(
        base_url="https://amojo.kommo.com",
        headers=_amojo_v1_headers(token),
        timeout=30.0,
    )


def get_amojo_client() -> httpx.AsyncClient:
    """Client para a API documentada do amojo (v2) usando HMAC-SHA1."""
    return httpx.AsyncClient(
        base_url="https://amojo.kommo.com",
        timeout=30.0,
    )
