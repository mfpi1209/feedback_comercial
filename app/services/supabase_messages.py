"""
Insere e enriquece mensagens na tabela mensagens_atendimento do Supabase.
Usa httpx (async) para chamar a REST API do Supabase diretamente.
"""

import logging
from datetime import datetime, timezone

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

_TIMEOUT = 15.0
_TABLE = "mensagens_atendimento_comercial"


def _is_configured() -> bool:
    s = get_settings()
    return bool(s.supabase_url and s.supabase_key)


def _headers(*, prefer: str = "return=representation") -> dict[str, str]:
    s = get_settings()
    return {
        "apikey": s.supabase_key,
        "Authorization": f"Bearer {s.supabase_key}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }


def _base_url() -> str:
    s = get_settings()
    return f"{s.supabase_url.rstrip('/')}/rest/v1/{_TABLE}"


async def insert_message_supabase(data: dict) -> dict | None:
    """
    Upsert de uma mensagem na tabela mensagens_atendimento.
    Usa message_uid como chave de conflito para idempotência.
    """
    if not _is_configured():
        return None

    url = _base_url()
    headers = _headers(prefer="return=representation,resolution=merge-duplicates")
    headers["Prefer"] = "return=representation,resolution=merge-duplicates"

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(url, headers=headers, json=data)

        if resp.status_code in (200, 201):
            result = resp.json()
            row = result[0] if isinstance(result, list) and result else result
            logger.debug("Mensagem inserida no Supabase: uid=%s", data.get("message_uid"))
            return row

        if resp.status_code == 409:
            logger.debug("Mensagem já existe no Supabase: uid=%s", data.get("message_uid"))
            return None

        logger.warning("Supabase insert %d: %s", resp.status_code, resp.text[:300])
        return None

    except Exception as exc:
        logger.warning("Erro ao inserir mensagem no Supabase: %s", exc)
        return None


async def enrich_atendimento_messages(
    atendimento_id: int,
    contact_id: int,
    session_start: datetime,
    session_end: datetime,
) -> int:
    """
    Atualiza mensagens no Supabase para um atendimento fechado:
    - Define atendimento_id
    - Calcula sequence_number (ordem cronológica)
    - Calcula response_time_seconds (delta entre mensagens consecutivas)

    Retorna a quantidade de mensagens enriquecidas.
    """
    if not _is_configured():
        return 0

    start_iso = session_start.isoformat() if session_start else None
    end_iso = session_end.isoformat() if session_end else None
    if not start_iso or not end_iso:
        return 0

    try:
        messages = await _fetch_messages_for_period(contact_id, start_iso, end_iso)
        if not messages:
            return 0

        messages.sort(key=lambda m: m.get("sent_at", ""))

        consultor_name = None
        for m in messages:
            if m.get("sender_type") == "user" and m.get("sender_name"):
                consultor_name = m["sender_name"]
                break

        prev_sent_at = None
        for seq, msg in enumerate(messages, start=1):
            updates: dict = {
                "atendimento_id": atendimento_id,
                "sequence_number": seq,
            }

            if consultor_name:
                updates["consultor_responsavel"] = consultor_name

            current_sent = msg.get("sent_at")
            if prev_sent_at and current_sent:
                try:
                    t1 = datetime.fromisoformat(prev_sent_at.replace("Z", "+00:00"))
                    t2 = datetime.fromisoformat(current_sent.replace("Z", "+00:00"))
                    delta = (t2 - t1).total_seconds()
                    updates["response_time_seconds"] = round(delta, 1)
                except (ValueError, TypeError):
                    pass

            prev_sent_at = current_sent

            await _patch_message(msg["message_uid"], updates)

        return len(messages)

    except Exception as exc:
        logger.exception("Erro ao enriquecer mensagens do atendimento %d: %s", atendimento_id, exc)
        return 0


async def _fetch_messages_for_period(
    contact_id: int,
    start_iso: str,
    end_iso: str,
) -> list[dict]:
    """Busca mensagens de um contact_id em um período no Supabase."""
    url = _base_url()
    params = {
        "select": "message_uid,sent_at,sender_type,sender_name",
        "contact_id": f"eq.{contact_id}",
        "sent_at": f"gte.{start_iso}",
        "order": "sent_at.asc",
    }

    headers = _headers(prefer="return=representation")

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(url, headers=headers, params=params)

        if resp.status_code == 200:
            rows = resp.json()
            return [r for r in rows if r.get("sent_at", "") <= end_iso]
        logger.warning("Supabase fetch mensagens %d: %s", resp.status_code, resp.text[:200])
        return []
    except Exception as exc:
        logger.warning("Erro ao buscar mensagens no Supabase: %s", exc)
        return []


async def _patch_message(message_uid: str, updates: dict) -> bool:
    """Atualiza campos de uma mensagem pelo message_uid."""
    url = _base_url() + f"?message_uid=eq.{message_uid}"
    headers = _headers(prefer="return=minimal")

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.patch(url, headers=headers, json=updates)
        return resp.status_code in range(200, 300)
    except Exception:
        return False
