"""
Rotas para expor cookies de sessão web do Kommo.

Permite que serviços externos (ex: DCz) obtenham os cookies
necessários para chamar endpoints AJAX internos do Kommo.
"""

from fastapi import APIRouter, HTTPException

from app.services.session_manager import get_cookies, is_configured
from app.services.token_renewer import renew_token_once, _is_configured as _pw_configured

router = APIRouter(prefix="/api/kommo/session", tags=["session"])


@router.get("")
async def get_session():
    """Retorna cookies de sessão atuais e flag de validade."""
    cookies = get_cookies()
    valid = is_configured()
    return {
        "is_valid": valid,
        "cookies": cookies if valid else {},
    }


@router.post("/renew")
async def renew_session():
    """Força renovação da sessão via Playwright e retorna cookies frescos."""
    if not _pw_configured():
        raise HTTPException(
            status_code=400,
            detail="KOMMO_LOGIN_EMAIL e KOMMO_LOGIN_PASSWORD nao configurados",
        )

    success = await renew_token_once()
    if not success:
        raise HTTPException(
            status_code=502,
            detail="Falha ao renovar sessao via Playwright",
        )

    cookies = get_cookies()
    return {
        "status": "ok",
        "is_valid": is_configured(),
        "cookies": cookies,
    }
