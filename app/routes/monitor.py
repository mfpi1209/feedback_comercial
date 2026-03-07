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


@router.get("/discovery-diag")
async def discovery_diagnostic(db: AsyncSession = Depends(get_db)):
    """
    Diagnóstico do discovery: busca 3 paginas da Talks API e compara
    com os chats monitorados. Não altera nada.
    """
    from app.config import get_settings
    from app.services.kommo_auth import get_bearer_client
    from app.services.rate_limiter import acquire_talks

    existing = await db.execute(select(MonitoredChat))
    known_ids = {r.chat_id for r in existing.scalars().all()}

    settings = get_settings()
    if not settings.kommo_access_token:
        return {"error": "KOMMO_ACCESS_TOKEN nao configurado"}

    pages_data = []
    total_talks = 0
    total_with_chat_id = 0
    total_without_chat_id = 0
    unique_chat_ids = set()
    new_chat_ids = set()
    sample_talks = []

    async with get_bearer_client() as client:
        for page_num in range(3):
            await acquire_talks()
            resp = await client.get("/api/v4/talks", params={
                "limit": 50,
                "offset": page_num * 50,
                "order[updated_at]": "desc",
                "filter[is_in_work]": "true",
            })

            if resp.status_code in (204, 404):
                break
            resp.raise_for_status()

            items = resp.json().get("_embedded", {}).get("talks", [])
            page_info = {"page": page_num, "items_count": len(items), "with_chat_id": 0, "without_chat_id": 0, "new": 0}

            for t in items:
                total_talks += 1
                chat_id = t.get("chat_id", "")
                if not chat_id:
                    total_without_chat_id += 1
                    page_info["without_chat_id"] += 1
                    if len(sample_talks) < 5:
                        sample_talks.append({
                            "talk_id": t.get("id"),
                            "chat_id": chat_id,
                            "contact_id": t.get("contact_id"),
                            "entity_id": t.get("entity_id"),
                            "entity_type": t.get("entity_type"),
                            "origin": t.get("origin"),
                            "is_in_work": t.get("is_in_work"),
                            "note": "SEM CHAT_ID",
                        })
                    continue

                total_with_chat_id += 1
                page_info["with_chat_id"] += 1
                unique_chat_ids.add(chat_id)

                if chat_id not in known_ids:
                    new_chat_ids.add(chat_id)
                    page_info["new"] += 1
                    if len(sample_talks) < 10:
                        sample_talks.append({
                            "talk_id": t.get("id"),
                            "chat_id": chat_id[:12],
                            "contact_id": t.get("contact_id"),
                            "entity_id": t.get("entity_id"),
                            "entity_type": t.get("entity_type"),
                            "origin": t.get("origin"),
                            "is_in_work": t.get("is_in_work"),
                            "note": "NOVO - nao monitorado",
                        })

            pages_data.append(page_info)

    return {
        "monitored_chats": len(known_ids),
        "talks_checked": total_talks,
        "with_chat_id": total_with_chat_id,
        "without_chat_id": total_without_chat_id,
        "unique_chat_ids_found": len(unique_chat_ids),
        "new_chat_ids": len(new_chat_ids),
        "pages": pages_data,
        "samples": sample_talks,
    }
