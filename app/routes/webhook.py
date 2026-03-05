"""
Webhook para receber mensagens de chat do Kommo em tempo real.
Configurar no Kommo: Settings → Integrations → Webhook URL

Suporta múltiplos formatos de payload do Kommo.
"""

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request

from app.database import async_session, dialect_insert
from app.models.message import KommoMessage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/kommo/webhook", tags=["webhook"])


@router.post("/messages")
async def receive_message_webhook(request: Request):
    """
    Recebe webhooks de mensagens do Kommo.
    Suporta formato interno v1 (com author/recipient/chat_id) e formato webhook padrão.
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
        raw = await request.body()
        logger.warning("Webhook recebido com body não-JSON: %s", raw[:500])

    logger.info("Webhook recebido: %s", _safe_summary(body))

    if isinstance(body, list):
        saved = 0
        for item in body:
            message_data = _extract_message(item)
            if message_data:
                saved += await _save_message(message_data)
        return {"status": "ok", "saved": saved}

    message_data = _extract_message(body)
    if not message_data:
        return {"status": "ignored", "reason": "not a chat message event"}

    await _save_message(message_data)
    return {"status": "ok"}


async def _save_message(message_data: dict) -> int:
    """Salva uma mensagem no banco. Retorna 1 se inserida, 0 se duplicada."""
    async with async_session() as db:
        stmt = (
            dialect_insert(KommoMessage)
            .values(**message_data)
            .on_conflict_do_nothing(index_elements=["message_uid"])
        )
        result = await db.execute(stmt)
        await db.commit()

        if result.rowcount and result.rowcount > 0:
            logger.info("Webhook: mensagem salva (uid=%s)", message_data.get("message_uid"))
            return 1
        logger.debug("Webhook: mensagem duplicada ignorada (uid=%s)", message_data.get("message_uid"))
        return 0


def _extract_message(body: dict) -> dict | None:
    """Detecta o formato do payload e delega ao parser correto."""
    if not isinstance(body, dict):
        return None

    if body.get("author") and body.get("chat_id"):
        return _parse_kommo_chat_message(body)

    msg = body.get("message", {})
    if isinstance(msg, dict) and "add" in msg:
        items = msg["add"]
        if isinstance(items, list) and items:
            return _parse_webhook_message(items[0])

    if "messages" in body:
        items = body["messages"]
        if isinstance(items, list) and items:
            return _parse_amojo_message(items[0], body)

    chat_message = body.get("chat_message", {})
    if chat_message:
        return _parse_chat_message_event(chat_message, body)

    return None


def _parse_kommo_chat_message(body: dict) -> dict | None:
    """
    Parse do formato interno v1 do Kommo.
    Estrutura confirmada via GET /v1/chats/{amojo_id}/{chat_id}/messages.

    author.origin:
      - "waba" → mensagem do contato (lead) via WhatsApp
      - "amocrm" → mensagem do consultor via painel Kommo
      - "bot" → mensagem automática do bot
    recipient: null quando a msg é do contato, preenchido quando é do consultor/bot
    """
    author = body.get("author") or {}
    recipient = body.get("recipient") or {}
    message = body.get("message") or {}
    dialog = body.get("dialog") or {}

    author_origin = author.get("origin", "")

    if author_origin == "amocrm":
        sender_type = "user"
    elif author_origin == "bot":
        sender_type = "bot"
    else:
        sender_type = "contact"

    sender_phone = _extract_phone(author, recipient, sender_type)

    msg_type = message.get("type", "text") if isinstance(message, dict) else "text"
    text = body.get("text") or (message.get("text") if isinstance(message, dict) else None)

    media_url = None
    if isinstance(message, dict):
        media_url = message.get("media") or None
        if media_url == "":
            media_url = None

    created_at = body.get("created_at", 0)

    contact_origin = "waba"
    if sender_type == "contact":
        contact_origin = author_origin or "waba"
    elif recipient:
        contact_origin = recipient.get("origin") or "waba"

    return {
        "lead_id": 0,
        "contact_id": None,
        "talk_id": str(dialog.get("id", "")),
        "chat_id": body.get("chat_id", ""),
        "sender_name": author.get("name") or author.get("full_name"),
        "sender_phone": sender_phone,
        "sender_type": sender_type,
        "message_text": text,
        "message_type": msg_type,
        "media_url": media_url,
        "sent_at": (
            datetime.fromtimestamp(created_at, tz=timezone.utc)
            if created_at
            else datetime.now(timezone.utc)
        ),
        "origin": contact_origin,
        "synced_at": datetime.now(timezone.utc),
        "message_uid": body.get("id"),
    }


def _extract_phone(author: dict, recipient: dict, sender_type: str) -> str | None:
    """Extrai telefone do contato a partir de origin_profile (wa_id)."""
    contact_data = author if sender_type == "contact" else (recipient or {})
    origin_profile_raw = contact_data.get("origin_profile", "")
    if not origin_profile_raw:
        return None
    try:
        profile = json.loads(origin_profile_raw) if isinstance(origin_profile_raw, str) else origin_profile_raw
        phone = profile.get("wa_id") or profile.get("phone")
        if phone and not phone.startswith("+"):
            phone = f"+{phone}"
        return phone
    except (json.JSONDecodeError, AttributeError):
        return None


def _parse_webhook_message(item: dict) -> dict | None:
    """Parse formato webhook padrão do Kommo (message[add])."""
    chat_id = item.get("chat_id", "")
    if not chat_id:
        return None

    created_at = item.get("created_at", 0)
    author = item.get("author", {})
    is_contact = author.get("type") == "contact"

    return {
        "lead_id": item.get("entity_id", 0),
        "contact_id": item.get("contact_id"),
        "talk_id": str(item.get("talk_id", "")),
        "chat_id": chat_id,
        "sender_name": author.get("name"),
        "sender_phone": None,
        "sender_type": "contact" if is_contact else "user",
        "message_text": item.get("text", ""),
        "message_type": item.get("type", "text"),
        "media_url": item.get("media"),
        "sent_at": (
            datetime.fromtimestamp(created_at, tz=timezone.utc)
            if created_at
            else datetime.now(timezone.utc)
        ),
        "origin": item.get("origin"),
        "synced_at": datetime.now(timezone.utc),
        "message_uid": item.get("id") or item.get("message_id"),
    }


def _parse_amojo_message(item: dict, body: dict) -> dict | None:
    """Parse formato amojo (messages array)."""
    sender = item.get("sender", {})
    msg = item.get("message", {})

    return {
        "lead_id": body.get("entity_id", 0),
        "contact_id": body.get("contact_id"),
        "talk_id": str(body.get("talk_id", "")),
        "chat_id": body.get("chat_id", ""),
        "sender_name": sender.get("name"),
        "sender_phone": sender.get("phone"),
        "sender_type": "contact" if sender.get("client_id") else "user",
        "message_text": msg.get("text"),
        "message_type": msg.get("type", "text"),
        "media_url": msg.get("media") or None,
        "sent_at": datetime.fromtimestamp(item.get("timestamp", 0), tz=timezone.utc),
        "origin": body.get("origin"),
        "synced_at": datetime.now(timezone.utc),
        "message_uid": msg.get("id"),
    }


def _parse_chat_message_event(chat_msg: dict, body: dict) -> dict | None:
    """Parse formato genérico de evento de chat."""
    return {
        "lead_id": body.get("entity_id", chat_msg.get("entity_id", 0)),
        "contact_id": body.get("contact_id", chat_msg.get("contact_id")),
        "talk_id": str(chat_msg.get("talk_id", "")),
        "chat_id": chat_msg.get("chat_id", ""),
        "sender_name": chat_msg.get("author", {}).get("name"),
        "sender_phone": None,
        "sender_type": "contact" if chat_msg.get("author", {}).get("type") == "contact" else "user",
        "message_text": chat_msg.get("text"),
        "message_type": chat_msg.get("type", "text"),
        "media_url": chat_msg.get("media"),
        "sent_at": (
            datetime.fromtimestamp(chat_msg.get("created_at", 0), tz=timezone.utc)
            if chat_msg.get("created_at")
            else datetime.now(timezone.utc)
        ),
        "origin": chat_msg.get("origin"),
        "synced_at": datetime.now(timezone.utc),
        "message_uid": str(chat_msg.get("id", chat_msg.get("message_id", ""))),
    }


def _safe_summary(body) -> str:
    """Resumo seguro do payload para logging."""
    if isinstance(body, list):
        return f"array[{len(body)} items]"
    if isinstance(body, dict):
        keys = list(body.keys())[:10]
        return f"keys={keys}"
    return str(type(body))
