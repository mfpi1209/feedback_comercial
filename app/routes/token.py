"""
Rotas para gerenciamento do amojo token.
Permite atualizar o token via API quando ele expirar,
sem precisar reiniciar o servidor.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.token_manager import get_token_state, update_token
from app.services.token_renewer import renew_token_once, _is_configured

router = APIRouter(prefix="/api/kommo/token", tags=["token"])


class TokenUpdate(BaseModel):
    token: str
    refresh_token: str = ""
    expired_at: int = 0


class TokenStatus(BaseModel):
    token: str
    refresh_token: str
    expired_at: int
    is_expired: bool
    seconds_until_expiry: int


@router.get("", response_model=TokenStatus)
async def check_token_status():
    """Verifica o estado atual do amojo token (sem expor o token completo)."""
    return get_token_state()


@router.put("")
async def set_token(body: TokenUpdate):
    """
    Atualiza o amojo token em runtime (sem reiniciar o servidor).
    Use quando o token expirar.

    Obter do Local Storage do Kommo: chave 'amojo_token'
    """
    update_token(
        token=body.token,
        refresh_token=body.refresh_token,
        expired_at=body.expired_at,
    )
    return {"status": "ok", "detail": "Token atualizado", **get_token_state()}


@router.get("/ensure-valid")
async def ensure_token_valid():
    """
    Verifica se o token esta valido. Se expirado, tenta renovar via Playwright.
    Ideal para o n8n chamar antes de processar eventos.
    """
    state = get_token_state()
    if not state["is_expired"]:
        return {"status": "ok", "action": "none", "detail": "Token valido", **state}

    if not _is_configured():
        return {
            "status": "warning",
            "action": "none",
            "detail": "Token expirado mas Playwright nao configurado",
            **state,
        }

    success = await renew_token_once()
    new_state = get_token_state()
    if success:
        return {"status": "ok", "action": "renewed", "detail": "Token renovado via Playwright", **new_state}
    return {"status": "error", "action": "failed", "detail": "Falha ao renovar token", **new_state}


@router.put("/renew")
async def renew_token_via_playwright():
    """
    Forca renovacao do amojo token via Playwright (login automatico no Kommo).
    Requer KOMMO_LOGIN_EMAIL e KOMMO_LOGIN_PASSWORD configurados no .env.
    """
    if not _is_configured():
        raise HTTPException(
            status_code=400,
            detail="KOMMO_LOGIN_EMAIL e KOMMO_LOGIN_PASSWORD nao configurados no .env",
        )

    success = await renew_token_once()
    if not success:
        raise HTTPException(
            status_code=502,
            detail="Falha ao renovar token via Playwright. Verifique credenciais e logs.",
        )

    return {"status": "ok", "detail": "Token renovado via Playwright", **get_token_state()}
