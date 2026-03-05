"""
Análise automática em batch de atendimentos fechados.

Roda em background, busca atendimentos com status "fechado" que ainda
nao foram analisados, e dispara o pipeline completo de IA + Supabase.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from sqlalchemy import select, update

from app.database import async_session
from app.models.analysis import AnalysisResultDB
from app.models.atendimento import Atendimento
from app.services.conversation_builder import build_conversation_by_atendimento
from app.services.ai_analyzer import run_full_analysis
from app.services.supabase_client import insert_feedback_comercial, build_feedback_row

logger = logging.getLogger(__name__)

ANALYZER_INTERVAL_SECONDS = 120
MAX_PER_CYCLE = 5
MIN_MESSAGES_TO_ANALYZE = 3


async def run_auto_analyzer():
    """Background loop que analisa atendimentos fechados automaticamente."""
    logger.info(
        "Auto-analyzer iniciado (intervalo=%ds, max_por_ciclo=%d, min_msgs=%d)",
        ANALYZER_INTERVAL_SECONDS, MAX_PER_CYCLE, MIN_MESSAGES_TO_ANALYZE,
    )

    await asyncio.sleep(60)

    while True:
        try:
            analyzed = await _analyze_pending_batch()
            if analyzed > 0:
                logger.info("Auto-analyzer: %d atendimentos analisados neste ciclo", analyzed)
        except Exception:
            logger.exception("Erro no auto-analyzer")
        await asyncio.sleep(ANALYZER_INTERVAL_SECONDS)


async def _analyze_pending_batch() -> int:
    """Busca e analisa atendimentos pendentes."""
    async with async_session() as db:
        pending_q = await db.execute(
            select(Atendimento)
            .where(
                Atendimento.status == "fechado",
                Atendimento.message_count >= MIN_MESSAGES_TO_ANALYZE,
            )
            .order_by(Atendimento.session_end.desc())
            .limit(MAX_PER_CYCLE)
        )
        pending = list(pending_q.scalars().all())

    analyzed = 0
    for atend in pending:
        try:
            success = await _analyze_single(atend)
            if success:
                analyzed += 1
        except Exception:
            logger.exception("Erro ao analisar atendimento %d", atend.id)
        await asyncio.sleep(2)

    return analyzed


async def _analyze_single(atend: Atendimento) -> bool:
    """Analisa um único atendimento."""
    logger.info(
        "Auto-analyzer: analisando atendimento %d (contact=%d, msgs=%d, chats=%s)",
        atend.id, atend.contact_id, atend.message_count,
        atend.chat_ids_json[:60] if atend.chat_ids_json else "[]",
    )

    conversation = await build_conversation_by_atendimento(atend.id)
    if conversation.message_count == 0:
        logger.warning("Atendimento %d sem mensagens — pulando", atend.id)
        return False

    analysis = await run_full_analysis(conversation)

    chat_ids_json = json.dumps(conversation.chat_ids)

    async with async_session() as db:
        db_row = AnalysisResultDB(
            chat_id=chat_ids_json,
            contact_id=atend.contact_id,
            atendimento_id=atend.id,
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
        db.add(db_row)
        await db.commit()
        await db.refresh(db_row)
        analysis_id = db_row.id

    try:
        row_data = build_feedback_row(
            analysis, chat_ids_json,
            lead_id=atend.lead_id,
            lead_nome=atend.lead_nome,
            lead_telefone=atend.lead_telefone,
            contact_id=atend.contact_id,
            atendimento_id=atend.id,
        )
        supa_result = await insert_feedback_comercial(row_data)
        if supa_result:
            async with async_session() as db:
                row = await db.get(AnalysisResultDB, analysis_id)
                if row:
                    row.supabase_synced = True
                    await db.commit()
    except Exception as e:
        logger.error("Erro Supabase para atendimento %d: %s", atend.id, e)

    async with async_session() as db:
        await db.execute(
            update(Atendimento)
            .where(Atendimento.id == atend.id)
            .values(status="analisado", updated_at=datetime.now(timezone.utc))
        )
        await db.commit()

    logger.info(
        "Atendimento %d analisado: consultor=%s, nota=%s, sentimento=%s",
        atend.id, analysis.consultor_responsavel, analysis.nota_atendimento, analysis.sentiment,
    )
    return True
