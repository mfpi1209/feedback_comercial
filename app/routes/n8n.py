"""Endpoints para monitorar o dispatcher n8n."""

from fastapi import APIRouter

from app.services.n8n_dispatcher import get_dispatcher

router = APIRouter(prefix="/api/n8n", tags=["n8n"])


@router.get("/queue")
async def queue_stats():
    """Status da fila de envio ao n8n — pendentes, enviados, falhas e historico."""
    return get_dispatcher().get_stats()
