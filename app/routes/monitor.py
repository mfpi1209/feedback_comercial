"""
Rotas para gerenciar chats monitorados ao vivo.
"""

from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.monitored_chat import MonitoredChat
from app.services.chat_discovery import discover_and_register_chats
from app.services.live_monitor import add_chat_to_monitor
from app.services.rate_limiter import get_usage

router = APIRouter(prefix="/api/kommo/monitor", tags=["monitor"])


class AddChatRequest(BaseModel):
    chat_id: str
    label: str | None = None
    lead_id: int | None = None
    initial_sync: bool = True


class ChatMonitorOut(BaseModel):
    id: int
    chat_id: str
    label: str | None
    lead_id: int | None
    last_message_uid: str | None
    last_message_at: datetime | None
    added_at: datetime
    active: bool

    model_config = {"from_attributes": True}


@router.post("/chats")
async def add_chat(body: AddChatRequest):
    """
    Adiciona um chat para monitoramento contínuo.
    Faz sync inicial das últimas 50 mensagens automaticamente.
    """
    result = await add_chat_to_monitor(
        chat_id=body.chat_id,
        label=body.label,
        lead_id=body.lead_id,
        do_initial_sync=body.initial_sync,
    )
    return result


@router.get("/chats", response_model=list[ChatMonitorOut])
async def list_monitored_chats(
    active_only: bool = True,
    db: AsyncSession = Depends(get_db),
):
    """Lista todos os chats monitorados."""
    stmt = select(MonitoredChat)
    if active_only:
        stmt = stmt.where(MonitoredChat.active == True)
    stmt = stmt.order_by(MonitoredChat.added_at.desc())
    result = await db.execute(stmt)
    return result.scalars().all()


@router.delete("/chats/{chat_id}")
async def remove_chat(chat_id: str, db: AsyncSession = Depends(get_db)):
    """Desativa o monitoramento de um chat (não apaga dados)."""
    await db.execute(
        update(MonitoredChat)
        .where(MonitoredChat.chat_id == chat_id)
        .values(active=False)
    )
    await db.commit()
    return {"status": "deactivated", "chat_id": chat_id}


@router.post("/discover")
async def trigger_discovery():
    """
    Dispara descoberta de chats agora (via Talks API).
    Requer KOMMO_ACCESS_TOKEN configurado.
    """
    new_count = await discover_and_register_chats()
    return {"status": "ok", "new_chats_found": new_count}


@router.get("/status")
async def monitor_status(db: AsyncSession = Depends(get_db)):
    """Retorna status geral do monitoramento."""
    total = await db.execute(
        select(MonitoredChat).where(MonitoredChat.active == True)
    )
    chats = total.scalars().all()

    return {
        "active_chats": len(chats),
        "rate_limit": get_usage(),
        "chats": [
            {
                "chat_id": c.chat_id,
                "label": c.label,
                "last_message_at": c.last_message_at.isoformat() if c.last_message_at else None,
            }
            for c in chats
        ],
    }
