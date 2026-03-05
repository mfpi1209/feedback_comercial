"""
Cliente Supabase para gravar resultados de análise na tabela feedback_comercial.
Usa httpx (async) para chamar a REST API do Supabase diretamente.
"""

import json
import logging

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

_TIMEOUT = 30.0


def _is_configured() -> bool:
    s = get_settings()
    return bool(s.supabase_url and s.supabase_key)


def _headers() -> dict[str, str]:
    s = get_settings()
    return {
        "apikey": s.supabase_key,
        "Authorization": f"Bearer {s.supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


async def insert_feedback_comercial(data: dict) -> dict | None:
    """
    Insere uma linha na tabela feedback_comercial do Supabase.
    Retorna o registro inserido ou None em caso de erro.
    """
    if not _is_configured():
        logger.warning("Supabase não configurado — pulando gravação")
        return None

    settings = get_settings()
    url = f"{settings.supabase_url.rstrip('/')}/rest/v1/feedback_comercial"

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(url, headers=_headers(), json=data)

            if resp.status_code in (200, 201):
                result = resp.json()
                row = result[0] if isinstance(result, list) and result else result
                logger.info("Feedback comercial inserido no Supabase: id=%s", row.get("id"))
                return row
            else:
                logger.error(
                    "Erro Supabase %d: %s", resp.status_code, resp.text[:500]
                )
                return None

    except Exception as e:
        logger.error("Erro ao inserir no Supabase: %s", e)
        return None


def build_feedback_row(
    analysis_result,
    chat_id: str,
    lead_id: int | None = None,
    lead_nome: str | None = None,
    lead_telefone: str | None = None,
    contact_id: int | None = None,
    atendimento_id: int | None = None,
) -> dict:
    """
    Converte um AnalysisResult em dict pronto para inserção no Supabase.
    Mapeia os campos da tabulação para as colunas REAIS da tabela feedback_comercial:

    chat_id, consultor, conversa, feedback, interesse, lead_id, lead_nome,
    lead_telefone, nota_abordagem, nota_atendimento, nota_conhecimento_produto,
    nv1, nv2, nv3, objecoes, ponto_negativo, ponto_positivo, status_lead,
    tempo_atendimento, tempo_medio_resposta, tempo_primeira_resposta, timestamp
    """
    from datetime import datetime, timezone

    tab = analysis_result.tabulation or {}
    summary = analysis_result.summary or {}
    metricas = tab.get("metricas_qualidade", {})
    feedback_data = tab.get("feedback", {})
    niveis = tab.get("tabela_niveis", {})

    row = {
        "chat_id": chat_id,
        "consultor": tab.get("consultor_responsavel", ""),
        "conversa": analysis_result.transcript,
        "feedback": _build_feedback_text(summary, feedback_data),
        "interesse": metricas.get("interesse_lead", ""),
        "nota_abordagem": _safe_float(metricas.get("qualidade_abordagem_nota")),
        "nota_atendimento": _safe_float(metricas.get("nota_atendimento")),
        "nota_conhecimento_produto": None,
        "nv1": niveis.get("nivel_1", ""),
        "nv2": niveis.get("nivel_2", ""),
        "nv3": niveis.get("nivel_3", ""),
        "objecoes": feedback_data.get("objecoes", ""),
        "ponto_positivo": tab.get("ponto_positivo", ""),
        "ponto_negativo": tab.get("ponto_negativo", ""),
        "status_lead": metricas.get("status_lead", ""),
        "tempo_atendimento": tab.get("tempo_atendimento", ""),
        "tempo_medio_resposta": tab.get("tempo_medio_resposta", ""),
        "tempo_primeira_resposta": tab.get("tempo_primeira_resposta", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if lead_id is not None:
        row["lead_id"] = lead_id
    if lead_nome:
        row["lead_nome"] = lead_nome
    if lead_telefone:
        row["lead_telefone"] = lead_telefone

    return row


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _build_feedback_text(summary: dict, feedback: dict) -> str:
    parts = []
    if summary.get("resumo"):
        parts.append(f"Resumo: {summary['resumo']}")
    if feedback.get("demanda"):
        parts.append(f"Demanda: {feedback['demanda']}")
    if feedback.get("proposta"):
        parts.append(f"Proposta: {feedback['proposta']}")
    if feedback.get("objecoes"):
        parts.append(f"Objeções: {feedback['objecoes']}")
    if feedback.get("resultado"):
        parts.append(f"Resultado: {feedback['resultado']}")
    return "\n".join(parts) if parts else ""
