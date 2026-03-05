"""
Endpoints para consulta e gestão de atendimentos detectados.
"""

import json
import logging

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select, func

from app.database import async_session
from app.models.atendimento import Atendimento

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/kommo/atendimentos", tags=["atendimentos"])


@router.get("/stats")
async def atendimento_stats():
    """Métricas gerais de atendimentos."""
    async with async_session() as db:
        total_q = await db.execute(select(func.count()).select_from(Atendimento))
        total = total_q.scalar() or 0

        by_status_q = await db.execute(
            select(Atendimento.status, func.count())
            .group_by(Atendimento.status)
        )
        by_status = {row[0]: row[1] for row in by_status_q.all()}

        avg_msgs_q = await db.execute(
            select(func.avg(Atendimento.message_count))
        )
        avg_msgs = avg_msgs_q.scalar()

        contacts_q = await db.execute(
            select(func.count(func.distinct(Atendimento.contact_id)))
        )
        unique_contacts = contacts_q.scalar() or 0

    return {
        "total_atendimentos": total,
        "by_status": by_status,
        "unique_contacts": unique_contacts,
        "avg_messages_per_atendimento": round(avg_msgs, 1) if avg_msgs else 0,
    }


@router.get("")
async def list_atendimentos(
    status: str | None = Query(None, description="Filtrar por status: aberto, fechado, analisado"),
    contact_id: int | None = Query(None, description="Filtrar por contact_id"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Lista atendimentos com filtros opcionais."""
    async with async_session() as db:
        query = select(Atendimento)
        count_query = select(func.count()).select_from(Atendimento)

        if status:
            query = query.where(Atendimento.status == status)
            count_query = count_query.where(Atendimento.status == status)
        if contact_id:
            query = query.where(Atendimento.contact_id == contact_id)
            count_query = count_query.where(Atendimento.contact_id == contact_id)

        total_q = await db.execute(count_query)
        total = total_q.scalar() or 0

        result = await db.execute(
            query.order_by(Atendimento.session_end.desc())
            .offset(offset)
            .limit(limit)
        )
        rows = result.scalars().all()

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "atendimentos": [_format_atendimento(a) for a in rows],
    }


@router.get("/{atendimento_id}")
async def get_atendimento(atendimento_id: int):
    """Detalhe de um atendimento com chat_ids e informações do lead."""
    async with async_session() as db:
        atend_q = await db.execute(
            select(Atendimento).where(Atendimento.id == atendimento_id)
        )
        atend = atend_q.scalar_one_or_none()

    if not atend:
        raise HTTPException(status_code=404, detail="Atendimento não encontrado")

    return _format_atendimento(atend)


def _format_atendimento(a: Atendimento) -> dict:
    chat_ids = json.loads(a.chat_ids_json) if a.chat_ids_json else []
    return {
        "id": a.id,
        "contact_id": a.contact_id,
        "lead_id": a.lead_id,
        "lead_nome": a.lead_nome,
        "lead_telefone": a.lead_telefone,
        "chat_ids": chat_ids,
        "chat_count": len(chat_ids),
        "session_start": str(a.session_start),
        "session_end": str(a.session_end),
        "message_count": a.message_count,
        "status": a.status,
        "created_at": str(a.created_at),
        "updated_at": str(a.updated_at),
    }
