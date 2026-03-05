"""
Rotas para gerenciamento do amojo token.
Permite atualizar o token via API quando ele expirar,
sem precisar reiniciar o servidor.
"""

from fastapi import APIRouter
from pydantic import BaseModel

from app.services.token_manager import get_token_state, update_token

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
