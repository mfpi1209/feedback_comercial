"""
Dashboard ao vivo para acompanhar mensagens sendo capturadas.
Acesse: GET /dashboard
"""

from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.message import KommoMessage
from app.models.monitored_chat import MonitoredChat
from app.services.rate_limiter import get_usage

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Dashboard HTML com auto-refresh para acompanhar mensagens ao vivo."""
    return HTMLResponse(DASHBOARD_HTML)


@router.get("/api/kommo/dashboard/stats")
async def dashboard_stats(db: AsyncSession = Depends(get_db)):
    """Dados para o dashboard: contadores e mensagens recentes."""
    total_msgs = await db.execute(select(func.count(KommoMessage.id)))
    total_chats = await db.execute(
        select(func.count(MonitoredChat.id)).where(MonitoredChat.active == True)
    )
    types = await db.execute(
        select(KommoMessage.message_type, func.count(KommoMessage.id))
        .group_by(KommoMessage.message_type)
    )

    recent = await db.execute(
        select(KommoMessage)
        .order_by(KommoMessage.sent_at.desc())
        .limit(30)
    )
    recent_msgs = recent.scalars().all()

    chats_active = await db.execute(
        select(
            KommoMessage.chat_id,
            func.count(KommoMessage.id).label("msg_count"),
            func.max(KommoMessage.sent_at).label("last_at"),
        )
        .group_by(KommoMessage.chat_id)
        .order_by(text("last_at DESC"))
        .limit(20)
    )

    monitored = await db.execute(select(MonitoredChat))
    labels = {m.chat_id: m.label for m in monitored.scalars().all()}

    return {
        "total_messages": total_msgs.scalar(),
        "active_chats": total_chats.scalar(),
        "rate_limit": get_usage(),
        "by_type": {r[0]: r[1] for r in types.all()},
        "recent_messages": [
            {
                "sent_at": m.sent_at.strftime("%d/%m %H:%M:%S") if m.sent_at else "",
                "sender_type": m.sender_type,
                "sender_name": m.sender_name or "?",
                "message_type": m.message_type,
                "text": (m.message_text or "")[:120],
                "chat_id": m.chat_id,
                "chat_label": labels.get(m.chat_id, m.chat_id[:12]),
            }
            for m in recent_msgs
        ],
        "top_chats": [
            {
                "chat_id": r[0],
                "label": labels.get(r[0], r[0][:12]),
                "msg_count": r[1],
                "last_at": r[2].strftime("%d/%m %H:%M:%S") if r[2] else "",
            }
            for r in chats_active.all()
        ],
    }


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Kommo Chat Monitor</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; padding: 20px; }
  h1 { font-size: 1.5rem; margin-bottom: 16px; color: #38bdf8; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .card { background: #1e293b; border-radius: 10px; padding: 16px; text-align: center; }
  .card .num { font-size: 2rem; font-weight: 700; color: #38bdf8; }
  .card .label { font-size: 0.8rem; color: #94a3b8; margin-top: 4px; }
  .section { margin-bottom: 20px; }
  .section h2 { font-size: 1.1rem; color: #94a3b8; margin-bottom: 8px; border-bottom: 1px solid #334155; padding-bottom: 6px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
  th { text-align: left; padding: 6px 8px; color: #64748b; font-weight: 600; border-bottom: 1px solid #334155; }
  td { padding: 6px 8px; border-bottom: 1px solid #1e293b; }
  tr:hover { background: #1e293b; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 0.75rem; font-weight: 600; }
  .badge-contact { background: #065f46; color: #6ee7b7; }
  .badge-user { background: #1e3a5f; color: #7dd3fc; }
  .badge-bot { background: #4a1d6e; color: #c4b5fd; }
  .badge-voice { background: #7c2d12; color: #fdba74; }
  .badge-picture { background: #365314; color: #bef264; }
  .badge-file { background: #3b3b1a; color: #fde047; }
  .rate { display: flex; align-items: center; gap: 8px; }
  .rate-bar { flex: 1; height: 8px; background: #334155; border-radius: 4px; overflow: hidden; }
  .rate-fill { height: 100%; background: #38bdf8; border-radius: 4px; transition: width 0.5s; }
  .rate-text { font-size: 0.8rem; color: #94a3b8; min-width: 80px; }
  .refresh-info { font-size: 0.75rem; color: #475569; text-align: right; margin-bottom: 8px; }
  .text-preview { max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
</style>
</head>
<body>

<h1>Kommo Chat Monitor</h1>
<div class="refresh-info" id="refreshInfo">Carregando...</div>

<div class="grid" id="cards"></div>

<div class="section">
  <h2>Rate Limit</h2>
  <div class="rate">
    <div class="rate-bar"><div class="rate-fill" id="rateBar"></div></div>
    <div class="rate-text" id="rateText"></div>
  </div>
</div>

<div class="section">
  <h2>Mensagens Recentes</h2>
  <table>
    <thead><tr><th>Hora</th><th>Chat</th><th>Quem</th><th>Tipo</th><th>Mensagem</th></tr></thead>
    <tbody id="recentTable"></tbody>
  </table>
</div>

<div class="section">
  <h2>Chats Mais Ativos</h2>
  <table>
    <thead><tr><th>Chat</th><th>Mensagens</th><th>Ultima msg</th></tr></thead>
    <tbody id="chatsTable"></tbody>
  </table>
</div>

<script>
function badge(type, text) {
  return `<span class="badge badge-${type}">${text}</span>`;
}

function senderBadge(t) {
  if (t === 'bot') return badge('bot', 'Bot');
  if (t === 'user') return badge('user', 'Consultor');
  return badge('contact', 'Lead');
}

function typeBadge(t) {
  if (t === 'voice') return badge('voice', 'audio');
  if (t === 'picture') return badge('picture', 'imagem');
  if (t === 'file') return badge('file', 'arquivo');
  return '';
}

async function refresh() {
  try {
    const r = await fetch('/api/kommo/dashboard/stats');
    const d = await r.json();

    document.getElementById('cards').innerHTML = `
      <div class="card"><div class="num">${d.total_messages}</div><div class="label">Mensagens</div></div>
      <div class="card"><div class="num">${d.active_chats}</div><div class="label">Chats Monitorados</div></div>
      <div class="card"><div class="num">${d.by_type.text || 0}</div><div class="label">Texto</div></div>
      <div class="card"><div class="num">${d.by_type.voice || 0}</div><div class="label">Audio</div></div>
      <div class="card"><div class="num">${d.by_type.picture || 0}</div><div class="label">Imagem</div></div>
      <div class="card"><div class="num">${d.by_type.file || 0}</div><div class="label">Arquivo</div></div>
    `;

    const rl = d.rate_limit;
    const pct = (rl.requests_last_minute / rl.max_rpm) * 100;
    document.getElementById('rateBar').style.width = pct + '%';
    document.getElementById('rateBar').style.background = pct > 80 ? '#ef4444' : '#38bdf8';
    document.getElementById('rateText').textContent = `${rl.requests_last_minute} / ${rl.max_rpm} rpm`;

    document.getElementById('recentTable').innerHTML = d.recent_messages.map(m => `
      <tr>
        <td>${m.sent_at}</td>
        <td>${m.chat_label}</td>
        <td>${senderBadge(m.sender_type)} ${m.sender_name}</td>
        <td>${typeBadge(m.message_type)}</td>
        <td class="text-preview">${m.text || '[' + m.message_type + ']'}</td>
      </tr>
    `).join('');

    document.getElementById('chatsTable').innerHTML = d.top_chats.map(c => `
      <tr>
        <td>${c.label}</td>
        <td>${c.msg_count}</td>
        <td>${c.last_at}</td>
      </tr>
    `).join('');

    document.getElementById('refreshInfo').textContent =
      'Atualizado: ' + new Date().toLocaleTimeString('pt-BR') + ' (auto-refresh 5s)';

  } catch(e) {
    document.getElementById('refreshInfo').textContent = 'Erro: ' + e.message;
  }
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"""
