"""
Endpoints para disparar e consultar análises IA de conversas comerciais.
"""

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select, func

from app.database import async_session
from app.models.analysis import AnalysisResultDB
from app.services.conversation_builder import build_conversation
from app.services.ai_analyzer import run_full_analysis
from app.services.supabase_client import insert_feedback_comercial, build_feedback_row

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/kommo/analysis", tags=["analysis"])


# ---- rotas fixas ANTES de /{chat_id} para evitar conflito ----

@router.get("/debug/supabase")
async def debug_supabase():
    """Testa a conexão com Supabase e verifica se está configurado."""
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

    return {
        "total_analyses": total,
        "supabase_synced": synced,
        "pending_sync": total - synced,
        "average_nota": round(avg_nota, 2) if avg_nota else None,
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


# ---- rotas dinâmicas com /{chat_id} ----

@router.post("/{chat_id}")
async def trigger_analysis(
    chat_id: str,
    hours: int | None = Query(None, description="Janela de horas (None = todas)"),
    force: bool = Query(False, description="Forçar re-análise mesmo se já existir"),
):
    """
    Dispara análise completa de um chat:
    1. Monta transcript enriquecido (com transcrição de mídias)
    2. Resumo via IA
    3. Análise de sentimento
    4. Tabulação comercial
    5. Salva no banco local + Supabase
    """
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
                    "detail": "Análise já existe. Use force=true para re-analisar.",
                    "analysis_id": row.id,
                    "analyzed_at": str(row.analyzed_at),
                    "consultor": row.consultor_responsavel,
                    "nota": row.nota_atendimento,
                    "sentiment": row.sentiment,
                }

    logger.info("Iniciando análise para chat %s (hours=%s, force=%s)", chat_id, hours, force)

    conversation = await build_conversation(chat_id, hours=hours)

    if conversation.message_count == 0:
        raise HTTPException(status_code=404, detail=f"Nenhuma mensagem encontrada para chat {chat_id}")

    analysis = await run_full_analysis(conversation)

    async with async_session() as session:
        db_row = AnalysisResultDB(
            chat_id=chat_id,
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

    lead_id = None
    lead_nome = None
    lead_telefone = None
    try:
        async with async_session() as session:
            from app.models.message import KommoMessage
            from app.models.monitored_chat import MonitoredChat

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

    supabase_ok = False
    try:
        row_data = build_feedback_row(
            analysis, chat_id,
            lead_id=lead_id, lead_nome=lead_nome, lead_telefone=lead_telefone,
        )
        supa_result = await insert_feedback_comercial(row_data)
        if supa_result:
            supabase_ok = True
            async with async_session() as session:
                db_row = await session.get(AnalysisResultDB, analysis_id)
                if db_row:
                    db_row.supabase_synced = True
                    await session.commit()
    except Exception as e:
        logger.error("Erro ao gravar no Supabase: %s", e)

    return {
        "status": "completed",
        "analysis_id": analysis_id,
        "chat_id": chat_id,
        "message_count": analysis.message_count,
        "media_count": analysis.media_count,
        "window_start": analysis.window_start,
        "window_end": analysis.window_end,
        "consultor_responsavel": analysis.consultor_responsavel,
        "nota_atendimento": analysis.nota_atendimento,
        "sentiment": analysis.sentiment,
        "summary": analysis.summary,
        "tabulation": analysis.tabulation,
        "supabase_synced": supabase_ok,
    }


@router.get("/{chat_id}")
async def get_analysis(chat_id: str):
    """Retorna a última análise salva de um chat."""
    async with async_session() as session:
        result = await session.execute(
            select(AnalysisResultDB)
            .where(AnalysisResultDB.chat_id == chat_id)
            .order_by(AnalysisResultDB.analyzed_at.desc())
            .limit(1)
        )
        row = result.scalars().first()

    if not row:
        raise HTTPException(status_code=404, detail=f"Nenhuma análise encontrada para chat {chat_id}")

    return {
        "analysis_id": row.id,
        "chat_id": row.chat_id,
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
