"""Endpoints para monitorar o dispatcher n8n."""

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.message import KommoMessage
from app.models.monitored_chat import MonitoredChat
from app.services.n8n_dispatcher import get_dispatcher

router = APIRouter(prefix="/api/n8n", tags=["n8n"])


@router.get("/queue")
async def queue_stats():
    """Status da fila de envio ao n8n — pendentes, enviados, falhas e historico."""
    return get_dispatcher().get_stats()


@router.post("/redispatch-all")
async def redispatch_all(db: AsyncSession = Depends(get_db)):
    """
    Re-despacha TODAS as mensagens do SQLite local para o n8n.
    Útil quando mensagens foram capturadas antes do dispatch estar ativo.
    """
    chat_rows = await db.execute(select(MonitoredChat))
    chat_map: dict[str, MonitoredChat] = {
        c.chat_id: c for c in chat_rows.scalars().all()
    }

    msg_rows = await db.execute(
        select(KommoMessage).order_by(KommoMessage.sent_at.asc())
    )
    messages = msg_rows.scalars().all()

    dispatcher = get_dispatcher()
    enqueued = 0

    for msg in messages:
        chat = chat_map.get(msg.chat_id)
        direction = "inbound" if msg.sender_type == "contact" else "outbound"
        consultor = msg.sender_name if msg.sender_type == "user" else None

        payload = {
            "event": "new_message",
            "message_uid": msg.message_uid,
            "chat_id": msg.chat_id,
            "contact_id": msg.contact_id or (chat.contact_id if chat else None),
            "lead_id": msg.lead_id or 0,
            "lead_nome": chat.label if chat else "",
            "direction": direction,
            "sender_type": msg.sender_type,
            "sender_name": msg.sender_name,
            "message_text": msg.message_text,
            "message_type": msg.message_type,
            "media_url": msg.media_url,
            "sent_at": msg.sent_at.isoformat() if msg.sent_at else None,
            "consultor_responsavel": consultor,
            "origin": msg.origin,
        }
        dispatcher.enqueue(payload)
        enqueued += 1

    return {
        "status": "ok",
        "enqueued": enqueued,
        "total_messages_in_db": len(messages),
    }
