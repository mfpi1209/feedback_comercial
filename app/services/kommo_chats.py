"""
Cliente para a API interna de chats do Kommo (amojo v1).
Busca histórico completo de mensagens usando x-auth-token.

Endpoint: GET /v1/chats/{amojo_id}/{chat_id}/messages?with_video=true&stand=v16
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from app.config import get_settings
from app.services.kommo_auth import get_amojo_v1_client

logger = logging.getLogger(__name__)


@dataclass
class ChatMessage:
    uid: str
    text: str | None
    message_type: str
    media_url: str | None
    media_file_name: str | None
    sender_id: str | None
    sender_name: str | None
    sender_phone: str | None
    sender_type: str  # "contact", "user" ou "bot"
    sender_origin: str | None
    delivery_status: int
    chat_id: str
    dialog_id: int | None
    external_id: str | None
    sent_at: datetime


def _parse_message(entry: dict) -> ChatMessage:
    """Converte um item do array de mensagens retornado pela API v1."""
    author = entry.get("author") or {}
    recipient = entry.get("recipient") or {}
    message = entry.get("message") or {}
    dialog = entry.get("dialog") or {}

    origin = author.get("origin", "")
    is_from_contact = origin not in ("amocrm",)
    is_bot = origin == "bot"

    if is_bot:
        sender_type = "bot"
    elif is_from_contact:
        sender_type = "contact"
    else:
        sender_type = "user"

    sender_phone = None
    contact_data = author if is_from_contact else recipient
    origin_profile_raw = contact_data.get("origin_profile", "")
    if origin_profile_raw:
        try:
            profile = json.loads(origin_profile_raw) if isinstance(origin_profile_raw, str) else origin_profile_raw
            sender_phone = profile.get("wa_id") or profile.get("phone")
            if sender_phone and not sender_phone.startswith("+"):
                sender_phone = f"+{sender_phone}"
        except (json.JSONDecodeError, AttributeError):
            pass

    msg_type = message.get("type", "text")
    media_url = message.get("media") or None
    if media_url == "":
        media_url = None

    created_at = entry.get("created_at", 0)
    sent_at = (
        datetime.fromtimestamp(created_at, tz=timezone.utc)
        if created_at
        else datetime.now(timezone.utc)
    )

    return ChatMessage(
        uid=entry.get("id", ""),
        text=entry.get("text") or message.get("text") or None,
        message_type=msg_type,
        media_url=media_url,
        media_file_name=message.get("media_file_name") or None,
        sender_id=author.get("id"),
        sender_name=author.get("name") or author.get("full_name"),
        sender_phone=sender_phone,
        sender_type=sender_type,
        sender_origin=origin or None,
        delivery_status=entry.get("delivery_status", -1),
        chat_id=entry.get("chat_id", ""),
        dialog_id=dialog.get("id"),
        external_id=entry.get("external_id"),
        sent_at=sent_at,
    )


async def fetch_chat_history(
    chat_id: str,
    limit: int = 50,
    offset: int = 0,
) -> list[ChatMessage]:
    """
    Busca uma página de mensagens via GET /v1/chats/{amojo_id}/{chat_id}/messages.
    Requer KOMMO_AMOJO_TOKEN configurado no .env.
    """
    settings = get_settings()
    from app.services.token_manager import get_current_token
    if not get_current_token():
        logger.warning("KOMMO_AMOJO_TOKEN não configurado — impossível buscar histórico")
        return []

    path = f"/v1/chats/{settings.kommo_amojo_id}/{chat_id}/messages"
    params = {
        "with_video": "true",
        "stand": "v16",
        "limit": limit,
        "offset": offset,
    }

    from app.services.rate_limiter import acquire
    from app.services.token_manager import force_refresh

    messages: list[ChatMessage] = []

    for attempt in range(2):
        async with await get_amojo_v1_client() as client:
            await acquire()
            resp = await client.get(path, params=params)

            if resp.status_code == 204:
                return []
            if resp.status_code == 401:
                if attempt == 0:
                    logger.warning("x-auth-token 401 — tentando refresh e retry...")
                    refreshed = await force_refresh()
                    if refreshed:
                        continue
                logger.error("x-auth-token expirado e refresh falhou. Atualize via PUT /api/kommo/token")
                try:
                    from app.services.token_renewer import request_emergency_renewal
                    request_emergency_renewal()
                except Exception:
                    pass
                return []
            if resp.status_code == 429:
                logger.warning("Rate limit 429 do Kommo! Aguardando 60s...")
                await asyncio.sleep(60)
                return []
            resp.raise_for_status()

            data = resp.json()
            items = data if isinstance(data, list) else data.get("messages", [])

            for entry in items:
                try:
                    messages.append(_parse_message(entry))
                except Exception:
                    logger.exception("Erro ao parsear mensagem: %s", entry.get("id", "?"))

            return messages

    return messages


async def fetch_full_chat_history(chat_id: str) -> list[ChatMessage]:
    """Busca TODAS as mensagens de um chat, paginando automaticamente."""
    all_messages: list[ChatMessage] = []
    offset = 0
    batch_size = 50

    while True:
        batch = await fetch_chat_history(chat_id, limit=batch_size, offset=offset)
        if not batch:
            break
        all_messages.extend(batch)
        if len(batch) < batch_size:
            break
        offset += batch_size

    logger.info("Chat %s: %d mensagens obtidas via amojo v1", chat_id, len(all_messages))
    return all_messages
