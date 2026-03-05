"""
Rotas para consulta e exportação de mensagens do Kommo.
"""

import logging
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import dialect_insert, get_db
from app.models.message import KommoMessage
from app.services.kommo_chats import fetch_full_chat_history
from app.services.sync_service import sync_all_messages, sync_messages_for_lead

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/kommo/messages", tags=["messages"])


# ── Schemas ──────────────────────────────────────────────────────────────────

class MessageOut(BaseModel):
    id: int
    lead_id: int
    contact_id: int | None
    talk_id: str | None
    chat_id: str
    sender_name: str | None
    sender_phone: str | None
    sender_type: str
    message_text: str | None
    message_type: str
    media_url: str | None
    sent_at: datetime
    origin: str | None
    synced_at: datetime

    model_config = {"from_attributes": True}


class SyncResult(BaseModel):
    status: str
    messages_synced: int | None = None
    detail: str | None = None


class ExportLine(BaseModel):
    timestamp: str
    origin: str | None
    sender_type: str
    sender_name: str | None
    text: str | None


class ExportResponse(BaseModel):
    lead_id: int | None
    chat_id: str | None
    total_messages: int
    conversation: list[ExportLine]
    plain_text: str


# ── GET /api/kommo/messages/by-lead/{lead_id} ──────────────────────────────

@router.get("/by-lead/{lead_id}", response_model=list[MessageOut])
async def get_messages_by_lead(
    lead_id: int,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    order: Literal["asc", "desc"] = "asc",
    db: AsyncSession = Depends(get_db),
):
    """Retorna mensagens armazenadas de um lead, ordenadas por data de envio."""
    order_col = KommoMessage.sent_at.asc() if order == "asc" else KommoMessage.sent_at.desc()
    stmt = (
        select(KommoMessage)
        .where(KommoMessage.lead_id == lead_id)
        .order_by(order_col)
        .limit(limit)
        .offset(offset)
    )
    result = await db.execute(stmt)
    return result.scalars().all()


# ── GET /api/kommo/messages/by-chat/{chat_id} ──────────────────────────────

@router.get("/by-chat/{chat_id}", response_model=list[MessageOut])
async def get_messages_by_chat(
    chat_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    order: Literal["asc", "desc"] = "asc",
    db: AsyncSession = Depends(get_db),
):
    """Retorna mensagens armazenadas de um chat_id, ordenadas por data de envio."""
    order_col = KommoMessage.sent_at.asc() if order == "asc" else KommoMessage.sent_at.desc()
    stmt = (
        select(KommoMessage)
        .where(KommoMessage.chat_id == chat_id)
        .order_by(order_col)
        .limit(limit)
        .offset(offset)
    )
    result = await db.execute(stmt)
    return result.scalars().all()


# ── GET /api/kommo/messages/export/{lead_id} ────────────────────────────────

@router.get("/export/{lead_id}", response_model=ExportResponse)
async def export_messages_for_ai(
    lead_id: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Exporta toda a conversa de um lead em formato texto limpo para alimentar IA.
    Retorna JSON com a lista estruturada + campo plain_text pronto para prompt.
    """
    stmt = (
        select(KommoMessage)
        .where(KommoMessage.lead_id == lead_id)
        .order_by(KommoMessage.sent_at.asc())
    )
    result = await db.execute(stmt)
    messages = result.scalars().all()

    lines, text_parts = _build_export(messages)

    return ExportResponse(
        lead_id=lead_id,
        chat_id=None,
        total_messages=len(messages),
        conversation=lines,
        plain_text="\n".join(text_parts),
    )


# ── GET /api/kommo/messages/export-chat/{chat_id} ──────────────────────────

@router.get("/export-chat/{chat_id}", response_model=ExportResponse)
async def export_chat_for_ai(
    chat_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Exporta toda a conversa de um chat_id em formato texto limpo para IA.
    Útil quando não se tem o lead_id, apenas o chat_id.
    """
    stmt = (
        select(KommoMessage)
        .where(KommoMessage.chat_id == chat_id)
        .order_by(KommoMessage.sent_at.asc())
    )
    result = await db.execute(stmt)
    messages = result.scalars().all()

    lines, text_parts = _build_export(messages)

    return ExportResponse(
        lead_id=None,
        chat_id=chat_id,
        total_messages=len(messages),
        conversation=lines,
        plain_text="\n".join(text_parts),
    )


# ── POST /api/kommo/messages/sync ──────────────────────────────────────────

@router.post("/sync", response_model=SyncResult)
async def trigger_full_sync(background_tasks: BackgroundTasks):
    """Dispara sync completo de todas as conversas em background."""
    background_tasks.add_task(sync_all_messages)
    return SyncResult(status="started", detail="Sync completo iniciado em background")


@router.post("/sync/{lead_id}", response_model=SyncResult)
async def trigger_lead_sync(
    lead_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Sincroniza mensagens de um lead específico (síncrono, via Talks + Chats API)."""
    count = await sync_messages_for_lead(lead_id, db)
    return SyncResult(status="completed", messages_synced=count)


# ── POST /api/kommo/messages/sync-chat/{chat_id} ───────────────────────────

@router.post("/sync-chat/{chat_id}", response_model=SyncResult)
async def trigger_chat_sync(
    chat_id: str,
    lead_id: int = Query(0, description="Lead ID para associar (0 se desconhecido)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Sincroniza mensagens diretamente por chat_id, sem precisar da Talks API.
    Usa o endpoint GET /v1/chats/{amojo_id}/{chat_id}/messages.
    """
    messages = await fetch_full_chat_history(chat_id)
    if not messages:
        return SyncResult(status="completed", messages_synced=0, detail="Nenhuma mensagem encontrada ou token não configurado")

    inserted = 0
    for msg in messages:
        if not msg.uid:
            continue
        stmt = (
            dialect_insert(KommoMessage)
            .values(
                lead_id=lead_id,
                contact_id=None,
                talk_id=str(msg.dialog_id or ""),
                chat_id=msg.chat_id or chat_id,
                sender_name=msg.sender_name,
                sender_phone=msg.sender_phone,
                sender_type=msg.sender_type,
                message_text=msg.text,
                message_type=msg.message_type,
                media_url=msg.media_url,
                sent_at=msg.sent_at,
                origin=msg.sender_origin,
                synced_at=datetime.now(timezone.utc),
                message_uid=msg.uid,
            )
            .on_conflict_do_nothing(index_elements=["message_uid"])
        )
        result = await db.execute(stmt)
        if result.rowcount and result.rowcount > 0:
            inserted += 1

    await db.commit()
    logger.info("Sync chat %s: %d novas mensagens (de %d total)", chat_id, inserted, len(messages))
    return SyncResult(status="completed", messages_synced=inserted)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _build_export(messages) -> tuple[list[ExportLine], list[str]]:
    lines: list[ExportLine] = []
    text_parts: list[str] = []

    for msg in messages:
        ts = msg.sent_at.strftime("%Y-%m-%d %H:%M:%S")
        if msg.sender_type == "bot":
            role = "Bot"
        elif msg.sender_type == "user":
            role = "Consultor"
        else:
            role = "Lead"
        name = msg.sender_name or role
        text = msg.message_text or f"[{msg.message_type}]"

        lines.append(
            ExportLine(
                timestamp=ts,
                origin=msg.origin,
                sender_type=msg.sender_type,
                sender_name=name,
                text=text,
            )
        )
        text_parts.append(f"[{ts}] {role} ({name}): {text}")

    return lines, text_parts
