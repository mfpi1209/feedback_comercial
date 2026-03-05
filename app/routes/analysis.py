"""
Endpoints para disparar e consultar análises IA de conversas comerciais.
Suporta análise por chat_id (legacy) e por atendimento (recomendado).
"""

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select, func, update

from app.database import async_session
from app.models.analysis import AnalysisResultDB
from app.models.atendimento import Atendimento
from app.services.conversation_builder import build_conversation, build_conversation_by_atendimento
from app.services.ai_analyzer import run_full_analysis
from app.services.supabase_client import insert_feedback_comercial, build_feedback_row

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/kommo/analysis", tags=["analysis"])


# ---- rotas fixas ----

@router.get("/debug/supabase")
async def debug_supabase():
    """Testa a conexão com Supabase."""
    from app.config import get_settings
    s = get_settings()
    configured = bool(s.supabase_url and s.supabase_key)
    result = {
        "configured": configured,
        "supabase_url_set": bool(s.supabase_url),
        "supabase_key_set": bool(s.supabase_key),
        "supabase_url_preview": s.supabase_url[:40] + "..." if s.supabase_url else "",
    }
    if configured:
        import httpx
        try:
            headers = {
                "apikey": s.supabase_key,
                "Authorization": f"Bearer {s.supabase_key}",
            }
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    f"{s.supabase_url}/rest/v1/feedback_comercial?select=id&limit=1",
                    headers=headers,
                )
                result["test_status"] = resp.status_code
                result["test_body"] = resp.text[:200]
        except Exception as e:
            result["test_error"] = str(e)
    return result


@router.get("/status/overview")
async def analysis_status():
    """Status geral das análises."""
    async with async_session() as session:
        total_q = await session.execute(
            select(func.count()).select_from(AnalysisResultDB)
        )
        total = total_q.scalar() or 0

        synced_q = await session.execute(
            select(func.count())
            .select_from(AnalysisResultDB)
            .where(AnalysisResultDB.supabase_synced == True)
        )
        synced = synced_q.scalar() or 0

        avg_q = await session.execute(
            select(func.avg(AnalysisResultDB.nota_atendimento))
            .where(AnalysisResultDB.nota_atendimento.isnot(None))
        )
        avg_nota = avg_q.scalar()

        atend_total_q = await session.execute(
            select(func.count()).select_from(Atendimento)
        )
        atend_total = atend_total_q.scalar() or 0

        atend_analyzed_q = await session.execute(
            select(func.count()).select_from(Atendimento)
            .where(Atendimento.status == "analisado")
        )
        atend_analyzed = atend_analyzed_q.scalar() or 0

        atend_pending_q = await session.execute(
            select(func.count()).select_from(Atendimento)
            .where(Atendimento.status == "fechado")
        )
        atend_pending = atend_pending_q.scalar() or 0

    return {
        "total_analyses": total,
        "supabase_synced": synced,
        "pending_sync": total - synced,
        "average_nota": round(avg_nota, 2) if avg_nota else None,
        "atendimentos_total": atend_total,
        "atendimentos_analisados": atend_analyzed,
        "atendimentos_pendentes": atend_pending,
    }


@router.get("/list")
async def list_analyses(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Lista todas as análises realizadas."""
    async with async_session() as session:
        count_q = await session.execute(
            select(func.count()).select_from(AnalysisResultDB)
        )
        total = count_q.scalar() or 0

        result = await session.execute(
            select(AnalysisResultDB)
            .order_by(AnalysisResultDB.analyzed_at.desc())
            .offset(offset)
            .limit(limit)
        )
        rows = result.scalars().all()

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "analyses": [
            {
                "analysis_id": r.id,
                "chat_id": r.chat_id,
                "contact_id": r.contact_id,
                "atendimento_id": r.atendimento_id,
                "analyzed_at": str(r.analyzed_at),
                "consultor": r.consultor_responsavel,
                "nota": r.nota_atendimento,
                "sentiment": r.sentiment,
                "messages": r.message_count,
                "supabase_synced": r.supabase_synced,
            }
            for r in rows
        ],
    }


# ---- rotas por atendimento (recomendado) ----

@router.post("/atendimento/{atendimento_id}")
async def trigger_atendimento_analysis(
    atendimento_id: int,
    force: bool = Query(False, description="Forçar re-análise"),
):
    """
    Dispara análise completa de um atendimento (multi-chat).
    Agrega mensagens de todos os chat_ids do atendimento.
    """
    async with async_session() as session:
        atend_q = await session.execute(
            select(Atendimento).where(Atendimento.id == atendimento_id)
        )
        atend = atend_q.scalar_one_or_none()

    if not atend:
        raise HTTPException(status_code=404, detail=f"Atendimento {atendimento_id} não encontrado")

    if not force:
        async with async_session() as session:
            existing = await session.execute(
                select(AnalysisResultDB)
                .where(AnalysisResultDB.atendimento_id == atendimento_id)
                .order_by(AnalysisResultDB.analyzed_at.desc())
                .limit(1)
            )
            row = existing.scalars().first()
            if row:
                return {
                    "status": "already_analyzed",
                    "detail": "Análise já existe. Use force=true para re-analisar.",
                    "analysis_id": row.id,
                    "atendimento_id": atendimento_id,
                    "consultor": row.consultor_responsavel,
                    "nota": row.nota_atendimento,
                }

    logger.info("Iniciando análise para atendimento %d (force=%s)", atendimento_id, force)

    conversation = await build_conversation_by_atendimento(atendimento_id)

    if conversation.message_count == 0:
        raise HTTPException(status_code=404, detail="Nenhuma mensagem encontrada para este atendimento")

    analysis = await run_full_analysis(conversation)

    chat_ids_json = json.dumps(conversation.chat_ids)
    analysis_id = await _save_analysis(
        analysis, chat_ids_json, atend.contact_id, atendimento_id,
        atend.lead_id, atend.lead_nome, atend.lead_telefone,
        conversation,
    )

    async with async_session() as session:
        await session.execute(
            update(Atendimento)
            .where(Atendimento.id == atendimento_id)
            .values(status="analisado", updated_at=datetime.now(timezone.utc))
        )
        await session.commit()

    return {
        "status": "completed",
        "analysis_id": analysis_id,
        "atendimento_id": atendimento_id,
        "contact_id": atend.contact_id,
        "chat_ids": conversation.chat_ids,
        "message_count": analysis.message_count,
        "media_count": analysis.media_count,
        "consultor_responsavel": analysis.consultor_responsavel,
        "nota_atendimento": analysis.nota_atendimento,
        "sentiment": analysis.sentiment,
        "summary": analysis.summary,
    }


@router.get("/atendimento/{atendimento_id}")
async def get_atendimento_analysis(atendimento_id: int):
    """Retorna a última análise de um atendimento."""
    async with async_session() as session:
        result = await session.execute(
            select(AnalysisResultDB)
            .where(AnalysisResultDB.atendimento_id == atendimento_id)
            .order_by(AnalysisResultDB.analyzed_at.desc())
            .limit(1)
        )
        row = result.scalars().first()

    if not row:
        raise HTTPException(status_code=404, detail="Nenhuma análise encontrada para este atendimento")

    return _format_analysis_row(row)


# ---- rotas legacy por chat_id ----

@router.post("/{chat_id}")
async def trigger_analysis(
    chat_id: str,
    hours: int | None = Query(None, description="Janela de horas (None = todas)"),
    force: bool = Query(False, description="Forçar re-análise"),
):
    """[Legacy] Dispara análise de um chat_id individual."""
    if not force:
        async with async_session() as session:
            existing = await session.execute(
                select(AnalysisResultDB)
                .where(AnalysisResultDB.chat_id == chat_id)
                .order_by(AnalysisResultDB.analyzed_at.desc())
                .limit(1)
            )
            row = existing.scalars().first()
            if row:
                return {
                    "status": "already_analyzed",
                    "detail": "Use /atendimento/{id} para análise multi-chat. Use force=true para re-analisar.",
                    "analysis_id": row.id,
                }

    conversation = await build_conversation(chat_id, hours=hours)
    if conversation.message_count == 0:
        raise HTTPException(status_code=404, detail=f"Nenhuma mensagem para chat {chat_id}")

    analysis = await run_full_analysis(conversation)

    lead_id, lead_nome, lead_telefone = await _get_lead_info_for_chat(chat_id)

    analysis_id = await _save_analysis(
        analysis, json.dumps([chat_id]), None, None,
        lead_id, lead_nome, lead_telefone, conversation,
    )

    return {
        "status": "completed",
        "analysis_id": analysis_id,
        "chat_id": chat_id,
        "message_count": analysis.message_count,
        "consultor_responsavel": analysis.consultor_responsavel,
        "nota_atendimento": analysis.nota_atendimento,
        "sentiment": analysis.sentiment,
        "summary": analysis.summary,
    }


@router.get("/{chat_id}")
async def get_analysis(chat_id: str):
    """[Legacy] Retorna a última análise de um chat_id."""
    async with async_session() as session:
        result = await session.execute(
            select(AnalysisResultDB)
            .where(AnalysisResultDB.chat_id == chat_id)
            .order_by(AnalysisResultDB.analyzed_at.desc())
            .limit(1)
        )
        row = result.scalars().first()

    if not row:
        raise HTTPException(status_code=404, detail=f"Nenhuma análise para chat {chat_id}")

    return _format_analysis_row(row)


# ---- helpers ----

async def _save_analysis(
    analysis, chat_id_value: str, contact_id, atendimento_id,
    lead_id, lead_nome, lead_telefone,
    conversation,
) -> int:
    """Salva resultado da análise no banco local e no Supabase."""
    async with async_session() as session:
        db_row = AnalysisResultDB(
            chat_id=chat_id_value,
            contact_id=contact_id,
            atendimento_id=atendimento_id,
            analyzed_at=datetime.now(timezone.utc),
            window_start=conversation.window_start,
            window_end=conversation.window_end,
            message_count=analysis.message_count,
            media_count=analysis.media_count,
            transcript=analysis.transcript,
            summary_json=json.dumps(analysis.summary, ensure_ascii=False),
            sentiment=analysis.sentiment,
            tabulation_json=json.dumps(analysis.tabulation, ensure_ascii=False),
            consultor_responsavel=analysis.consultor_responsavel,
            nota_atendimento=analysis.nota_atendimento,
            supabase_synced=False,
        )
        session.add(db_row)
        await session.commit()
        await session.refresh(db_row)
        analysis_id = db_row.id

    try:
        row_data = build_feedback_row(
            analysis, chat_id_value,
            lead_id=lead_id, lead_nome=lead_nome, lead_telefone=lead_telefone,
            contact_id=contact_id, atendimento_id=atendimento_id,
        )
        supa_result = await insert_feedback_comercial(row_data)
        if supa_result:
            async with async_session() as session:
                db_row = await session.get(AnalysisResultDB, analysis_id)
                if db_row:
                    db_row.supabase_synced = True
                    await session.commit()
    except Exception as e:
        logger.error("Erro ao gravar no Supabase: %s", e)

    return analysis_id


async def _get_lead_info_for_chat(chat_id: str) -> tuple:
    """Busca lead_id, nome e telefone para um chat."""
    from app.models.message import KommoMessage
    from app.models.monitored_chat import MonitoredChat

    lead_id = None
    lead_nome = None
    lead_telefone = None
    try:
        async with async_session() as session:
            lead_row = await session.execute(
                select(KommoMessage.lead_id, KommoMessage.sender_name, KommoMessage.sender_phone)
                .where(KommoMessage.chat_id == chat_id, KommoMessage.sender_type == "contact")
                .limit(1)
            )
            row = lead_row.first()
            if row:
                lead_id = row.lead_id if row.lead_id else None
                lead_nome = row.sender_name
                lead_telefone = row.sender_phone

            if not lead_id:
                mon_row = await session.execute(
                    select(MonitoredChat.lead_id)
                    .where(MonitoredChat.chat_id == chat_id, MonitoredChat.lead_id.isnot(None), MonitoredChat.lead_id != 0)
                    .limit(1)
                )
                mon = mon_row.scalar()
                if mon:
                    lead_id = mon
    except Exception:
        pass
    return lead_id, lead_nome, lead_telefone


def _format_analysis_row(row: AnalysisResultDB) -> dict:
    return {
        "analysis_id": row.id,
        "chat_id": row.chat_id,
        "contact_id": row.contact_id,
        "atendimento_id": row.atendimento_id,
        "analyzed_at": str(row.analyzed_at),
        "message_count": row.message_count,
        "media_count": row.media_count,
        "window_start": str(row.window_start) if row.window_start else None,
        "window_end": str(row.window_end) if row.window_end else None,
        "consultor_responsavel": row.consultor_responsavel,
        "nota_atendimento": row.nota_atendimento,
        "sentiment": row.sentiment,
        "summary": json.loads(row.summary_json) if row.summary_json else None,
        "tabulation": json.loads(row.tabulation_json) if row.tabulation_json else None,
        "transcript": row.transcript,
        "supabase_synced": row.supabase_synced,
    }
