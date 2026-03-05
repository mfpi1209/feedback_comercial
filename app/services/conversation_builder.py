"""
Monta o transcript enriquecido de uma conversa:
- Busca mensagens do banco
- Processa mídias (transcrição/descrição via IA)
- Produz texto formatado pronto para análise
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.database import async_session
from app.models.message import KommoMessage
from app.services.media_processor import process_media

logger = logging.getLogger(__name__)

MAX_CONCURRENT_MEDIA = 5


@dataclass
class ConversationData:
    chat_id: str
    transcript: str = ""
    message_count: int = 0
    media_count: int = 0
    window_start: datetime | None = None
    window_end: datetime | None = None
    consultor_names: list[str] = field(default_factory=list)
    cliente_names: list[str] = field(default_factory=list)
    messages_raw: list[dict] = field(default_factory=list)


def _classify_sender(sender_type: str, sender_name: str | None) -> tuple[str, str]:
    """Retorna (papel, nome) baseado no sender_type do Kommo."""
    name = (sender_name or "").strip()
    st = (sender_type or "").lower()

    if st == "contact":
        return "CLIENTE", name or "Cliente"
    elif st == "user":
        return "CONSULTOR", name or "Consultor"
    elif st == "bot":
        return "BOT", name or "Bot"
    else:
        return "SISTEMA", name or "Sistema"


async def _process_media_with_semaphore(
    sem: asyncio.Semaphore,
    msg_type: str,
    media_url: str,
) -> str:
    async with sem:
        return await process_media(msg_type, media_url)


async def build_conversation(
    chat_id: str,
    hours: int | None = None,
) -> ConversationData:
    """
    Busca mensagens do chat no banco, processa mídias e monta transcript.

    Args:
        chat_id: ID do chat
        hours: Janela de horas (None = todas as mensagens)
    """
    result = ConversationData(chat_id=chat_id)

    async with async_session() as session:
        query = (
            select(KommoMessage)
            .where(KommoMessage.chat_id == chat_id)
            .order_by(KommoMessage.sent_at.asc())
        )

        if hours:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
            query = query.where(KommoMessage.sent_at >= cutoff)

        rows = await session.execute(query)
        messages = rows.scalars().all()

    if not messages:
        result.transcript = "Nenhuma mensagem encontrada."
        return result

    result.message_count = len(messages)
    result.window_start = messages[0].sent_at
    result.window_end = messages[-1].sent_at

    sem = asyncio.Semaphore(MAX_CONCURRENT_MEDIA)
    media_tasks: dict[int, asyncio.Task] = {}

    for i, msg in enumerate(messages):
        if msg.message_type and msg.message_type.lower() != "text" and msg.media_url:
            task = asyncio.create_task(
                _process_media_with_semaphore(sem, msg.message_type, msg.media_url)
            )
            media_tasks[i] = task
            result.media_count += 1

    if media_tasks:
        await asyncio.gather(*media_tasks.values(), return_exceptions=True)

    lines: list[str] = []
    consultor_set: set[str] = set()
    cliente_set: set[str] = set()

    for i, msg in enumerate(messages):
        papel, nome = _classify_sender(msg.sender_type, msg.sender_name)

        if papel == "CONSULTOR" and nome != "Consultor":
            consultor_set.add(nome)
        elif papel == "CLIENTE" and nome != "Cliente":
            cliente_set.add(nome)

        if i in media_tasks:
            task = media_tasks[i]
            if task.done() and not task.cancelled():
                exc = task.exception()
                if exc:
                    content = f"[Mídia: erro - {exc}]"
                else:
                    content = task.result()
            else:
                content = f"[{msg.message_type}]"
        else:
            content = msg.message_text or ""

        ts = msg.sent_at.strftime("%Y-%m-%dT%H:%M:%S") if msg.sent_at else "?"
        label = f"{papel} ({nome})" if nome else papel
        lines.append(f"[{ts}] {label}: {content}")

        result.messages_raw.append({
            "message_uid": msg.message_uid,
            "sent_at": str(msg.sent_at),
            "sender_type": papel,
            "sender_name": nome,
            "content": content,
            "message_type": msg.message_type,
            "has_media": bool(i in media_tasks),
        })

    result.transcript = "\n".join(lines)
    result.consultor_names = sorted(consultor_set)
    result.cliente_names = sorted(cliente_set)

    logger.info(
        "Conversa %s: %d msgs, %d mídias processadas, %d chars transcript",
        chat_id, result.message_count, result.media_count, len(result.transcript),
    )
    return result
