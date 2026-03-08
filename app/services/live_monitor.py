"""
Monitor ao vivo de mensagens do Kommo.

Usa polling por camadas de atividade para minimizar requests:
  HOT    (msg < 30 min)  → poll a cada 1 min
  WARM   (msg < 6h)      → poll a cada 20 min
  COLD   (msg < 48h)     → sem polling (discovery promove pra HOT se houver atividade)
  FROZEN (msg > 48h)     → sem polling (discovery promove pra HOT se houver atividade)
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session, dialect_insert
from app.models.message import KommoMessage
from app.models.monitored_chat import MonitoredChat
from app.services.kommo_chats import fetch_chat_history, fetch_full_chat_history
from app.services.rate_limiter import get_usage
from app.services.token_manager import get_current_token
from app.services.n8n_dispatcher import get_dispatcher

logger = logging.getLogger(__name__)

# ── Polling tiers ────────────────────────────────────────────────────────────
# (name, max_age_of_last_message, min_interval_between_polls)
SKIP_TIERS = {"cold", "frozen"}

TIERS: list[tuple[str, timedelta, timedelta]] = [
    ("hot",    timedelta(minutes=30), timedelta(minutes=1)),
    ("warm",   timedelta(hours=6),    timedelta(minutes=20)),
    ("cold",   timedelta(hours=48),   timedelta(0)),
    ("frozen", timedelta(days=3650),  timedelta(0)),
]

_tier_stats: dict[str, int] = {"hot": 0, "warm": 0, "cold": 0, "frozen": 0}


def get_tier_stats() -> dict[str, int]:
    return dict(_tier_stats)


def _ensure_aware(dt: datetime | None) -> datetime | None:
    """Garante que um datetime é timezone-aware (UTC)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _classify_chat(chat: MonitoredChat, now: datetime) -> tuple[str, timedelta]:
    """Returns (tier_name, min_poll_interval) for a chat."""
    last_msg = _ensure_aware(chat.last_message_at)
    if last_msg is None:
        return "hot", timedelta(seconds=0)

    age = now - last_msg
    for name, max_age, interval in TIERS:
        if age < max_age:
            return name, interval
    return "frozen", TIERS[-1][2]


def _is_due_for_poll(chat: MonitoredChat, now: datetime) -> tuple[bool, str]:
    """Check if a chat should be polled this cycle."""
    tier, interval = _classify_chat(chat, now)

    if interval == timedelta(seconds=0):
        return True, tier

    last_polled = _ensure_aware(chat.last_polled_at)
    if last_polled is None:
        return True, tier

    elapsed = now - last_polled
    return elapsed >= interval, tier


def _build_message_payload(msg, chat) -> dict:
    """Monta o payload da mensagem para n8n com metadados enriquecidos do inbox."""
    direction = "inbound" if msg.sender_type == "contact" else "outbound"
    consultor = msg.sender_name if msg.sender_type == "user" else None
    sent_at_iso = msg.sent_at.isoformat() if msg.sent_at else None

    return {
        "message_uid": msg.uid,
        "chat_id": msg.chat_id or chat.chat_id,
        "contact_id": chat.contact_id,
        "lead_id": chat.lead_id or 0,
        "lead_nome": getattr(chat, "lead_nome", None) or chat.label,
        "contact_name": getattr(chat, "contact_name", None),
        "direction": direction,
        "sender_type": msg.sender_type,
        "sender_name": msg.sender_name,
        "message_text": msg.text,
        "message_type": msg.message_type,
        "media_url": msg.media_url,
        "sent_at": sent_at_iso,
        "consultor_responsavel": consultor,
        "origin": msg.sender_origin or getattr(chat, "chat_source", None),
        "responsible_user_id": getattr(chat, "responsible_user_id", None),
        "pipeline_id": getattr(chat, "pipeline_id", None),
        "status_id": getattr(chat, "status_id", None),
        "chat_source": getattr(chat, "chat_source", None),
    }


def _dispatch_message(payload: dict) -> None:
    """Enfileira mensagem no dispatcher centralizado para o n8n."""
    get_dispatcher().enqueue({"event": "new_message", **payload})


MIN_CYCLE_SECONDS = 2
CONCURRENT_POLLS = 8

_cycle_count = 0
_total_active = 0
_semaphore: asyncio.Semaphore | None = None


async def run_live_monitor():
    """Loop principal — seleciona apenas chats elegíveis por camada a cada ciclo."""
    global _cycle_count, _total_active, _semaphore
    _semaphore = asyncio.Semaphore(CONCURRENT_POLLS)
    logger.info(
        "Monitor ao vivo iniciado (parallel=%d, ciclo_min=%ds, tiers=%s)",
        CONCURRENT_POLLS, MIN_CYCLE_SECONDS,
        [(t[0], str(t[2])) for t in TIERS],
    )

    while True:
        t0 = asyncio.get_event_loop().time()
        try:
            if not get_current_token():
                logger.debug("Monitor: sem amojo token, aguardando...")
                await asyncio.sleep(MIN_CYCLE_SECONDS)
                continue

            _cycle_count += 1

            async with async_session() as db:
                due_chats, total, stats = await _get_chats_due_for_poll(db)
                _total_active = total
                _tier_stats.update(stats)

            if due_chats:
                total_new = await _poll_all_parallel(due_chats)
                elapsed = asyncio.get_event_loop().time() - t0

                logger.info(
                    "Monitor ciclo #%d: %d/%d chats pollados, %d novas msgs, "
                    "%.1fs | tiers=%s | %s",
                    _cycle_count, len(due_chats), total, total_new,
                    elapsed, stats, get_usage(),
                )

        except Exception:
            logger.exception("Erro no loop do monitor")

        elapsed = asyncio.get_event_loop().time() - t0
        sleep_time = max(MIN_CYCLE_SECONDS - elapsed, 0.5)
        await asyncio.sleep(sleep_time)


async def _poll_all_parallel(chats: list) -> int:
    """Faz polling dos chats elegíveis em paralelo, limitado pelo semaforo."""
    results = await asyncio.gather(
        *[_poll_chat_safe(chat) for chat in chats],
        return_exceptions=True,
    )
    return sum(r for r in results if isinstance(r, int))


async def _poll_chat_safe(chat) -> int:
    """Poll com semaforo para limitar concorrencia."""
    async with _semaphore:
        try:
            return await _poll_chat(chat)
        except Exception:
            logger.exception("Erro ao monitorar chat %s", chat.chat_id)
            return 0


async def _get_chats_due_for_poll(
    db: AsyncSession,
) -> tuple[list[MonitoredChat], int, dict[str, int]]:
    """
    Retorna apenas os chats cujo intervalo de polling expirou.
    Returns: (chats_to_poll, total_active, tier_counts)
    """
    result = await db.execute(
        select(MonitoredChat)
        .where(MonitoredChat.active == True)
        .order_by(MonitoredChat.last_message_at.desc().nullslast())
    )
    all_active = list(result.scalars().all())

    now = datetime.now(timezone.utc)
    due: list[MonitoredChat] = []
    counts: dict[str, int] = {"hot": 0, "warm": 0, "cold": 0, "frozen": 0}

    for chat in all_active:
        is_due, tier = _is_due_for_poll(chat, now)
        counts[tier] = counts.get(tier, 0) + 1
        if tier in SKIP_TIERS:
            continue
        if is_due:
            due.append(chat)

    return due, len(all_active), counts


async def _poll_chat(chat: MonitoredChat) -> int:
    """
    Verifica um chat por mensagens novas. Nunca perde uma mensagem:
    - Primeiro poll (sem ponteiro): busca historico COMPLETO com paginacao
    - Polls seguintes: busca 50 mais recentes, pagina se necessario
    Despacha tudo em ordem cronologica para o n8n.
    """
    if chat.last_message_uid is None:
        messages = await fetch_full_chat_history(chat.chat_id)
        logger.info(
            "Primeiro poll de %s (%s): %d mensagens historicas",
            chat.chat_id[:12], chat.label or "sem label", len(messages),
        )
    else:
        messages = await fetch_chat_history(chat.chat_id, limit=50, offset=0)

    if not messages:
        return 0

    new_msgs = []
    found_marker = False
    for msg in messages:
        if not msg.uid:
            continue
        if chat.last_message_uid and msg.uid == chat.last_message_uid:
            found_marker = True
            break
        new_msgs.append(msg)

    if chat.last_message_uid and not found_marker and len(messages) >= 50:
        logger.warning(
            "Chat %s: >50 msgs novas, paginando para nao perder nenhuma...",
            chat.chat_id[:12],
        )
        offset = 50
        while not found_marker:
            more = await fetch_chat_history(chat.chat_id, limit=50, offset=offset)
            if not more:
                break
            for msg in more:
                if not msg.uid:
                    continue
                if msg.uid == chat.last_message_uid:
                    found_marker = True
                    break
                new_msgs.append(msg)
            if len(more) < 50:
                break
            offset += 50

    now = datetime.now(timezone.utc)

    if not new_msgs:
        async with async_session() as db:
            await db.execute(
                update(MonitoredChat)
                .where(MonitoredChat.id == chat.id)
                .values(last_polled_at=now)
            )
            await db.commit()
        return 0

    latest_uid = new_msgs[0].uid
    latest_at = new_msgs[0].sent_at

    new_msgs.reverse()

    inserted = 0
    async with async_session() as db:
        for msg in new_msgs:
            stmt = (
                dialect_insert(KommoMessage)
                .values(
                    lead_id=chat.lead_id or 0,
                    contact_id=chat.contact_id,
                    talk_id=str(msg.dialog_id or ""),
                    chat_id=msg.chat_id or chat.chat_id,
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
                _dispatch_message(_build_message_payload(msg, chat))

        update_values = {"last_polled_at": now}
        if inserted > 0 or chat.last_message_uid is None:
            update_values["last_message_uid"] = latest_uid
            update_values["last_message_at"] = latest_at

        await db.execute(
            update(MonitoredChat)
            .where(MonitoredChat.id == chat.id)
            .values(**update_values)
        )

        await db.commit()

    if inserted > 0:
        logger.info(
            "Chat %s (%s): %d novas mensagens",
            chat.chat_id[:12], chat.label or "sem label", inserted,
        )

    return inserted


async def add_chat_to_monitor(
    chat_id: str,
    label: str | None = None,
    lead_id: int | None = None,
    contact_id: int | None = None,
    do_initial_sync: bool = True,
) -> dict:
    """
    Registra um chat para monitoramento contínuo.
    Opcionalmente faz sync inicial das mensagens existentes.
    """
    async with async_session() as db:
        existing = await db.execute(
            select(MonitoredChat).where(MonitoredChat.chat_id == chat_id)
        )
        chat = existing.scalar_one_or_none()

        if chat:
            if not chat.active:
                chat.active = True
                if label:
                    chat.label = label
                if lead_id is not None:
                    chat.lead_id = lead_id
                await db.commit()
                return {"status": "reactivated", "chat_id": chat_id}
            return {"status": "already_monitored", "chat_id": chat_id}

        new_chat = MonitoredChat(
            chat_id=chat_id,
            label=label,
            lead_id=lead_id,
            contact_id=contact_id,
            active=True,
        )
        db.add(new_chat)
        await db.commit()

    if do_initial_sync:
        messages = await fetch_chat_history(chat_id, limit=50)
        if messages:
            count = 0
            async with async_session() as db:
                for msg in messages:
                    if not msg.uid:
                        continue
                    stmt = (
                        dialect_insert(KommoMessage)
                        .values(
                            lead_id=lead_id or 0,
                            contact_id=contact_id,
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
                        count += 1
                        direction = "inbound" if msg.sender_type == "contact" else "outbound"
                        consultor = msg.sender_name if msg.sender_type == "user" else None
                        payload = {
                            "message_uid": msg.uid,
                            "chat_id": msg.chat_id or chat_id,
                            "contact_id": contact_id,
                            "lead_id": lead_id or 0,
                            "lead_nome": label,
                            "direction": direction,
                            "sender_type": msg.sender_type,
                            "sender_name": msg.sender_name,
                            "message_text": msg.text,
                            "message_type": msg.message_type,
                            "media_url": msg.media_url,
                            "sent_at": msg.sent_at.isoformat() if msg.sent_at else None,
                            "consultor_responsavel": consultor,
                            "origin": msg.sender_origin,
                        }
                        _dispatch_message(payload)

                if messages:
                    await db.execute(
                        update(MonitoredChat)
                        .where(MonitoredChat.chat_id == chat_id)
                        .values(
                            last_message_uid=messages[0].uid,
                            last_message_at=messages[0].sent_at,
                        )
                    )
                await db.commit()

            logger.info("Chat %s: sync inicial = %d mensagens", chat_id, count)
            return {"status": "added", "chat_id": chat_id, "initial_sync": count}

    return {"status": "added", "chat_id": chat_id, "initial_sync": 0}
