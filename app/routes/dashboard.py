"""
Dashboard ao vivo do Kommo Dispatcher.
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
from app.services.n8n_dispatcher import get_dispatcher
from app.services.token_manager import get_token_state

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


@router.get("/api/kommo/dashboard/stats")
async def dashboard_stats(db: AsyncSession = Depends(get_db)):
    total_msgs = await db.execute(select(func.count(KommoMessage.id)))
    total_chats = await db.execute(
        select(func.count(MonitoredChat.id)).where(MonitoredChat.active == True)
    )
    synced_chats = await db.execute(
        select(func.count(MonitoredChat.id)).where(
            MonitoredChat.active == True,
            MonitoredChat.last_message_uid.isnot(None),
        )
    )
    pending_chats = await db.execute(
        select(func.count(MonitoredChat.id)).where(
            MonitoredChat.active == True,
            MonitoredChat.last_message_uid.is_(None),
        )
    )
    types = await db.execute(
        select(KommoMessage.message_type, func.count(KommoMessage.id))
        .group_by(KommoMessage.message_type)
    )

    recent = await db.execute(
        select(KommoMessage)
        .order_by(KommoMessage.sent_at.desc())
        .limit(50)
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

    n8n = get_dispatcher().get_stats()

    token = get_token_state()
    token_info = {
        "token_preview": (token.get("token") or "")[:20] + "..." if token.get("token") else "nao configurado",
        "is_expired": token.get("is_expired", True),
        "seconds_until_expiry": token.get("seconds_until_expiry", 0),
    }

    return {
        "total_messages": total_msgs.scalar(),
        "active_chats": total_chats.scalar(),
        "synced_chats": synced_chats.scalar(),
        "pending_sync_chats": pending_chats.scalar(),
        "rate_limit": get_usage(),
        "by_type": {r[0]: r[1] for r in types.all()},
        "n8n": n8n,
        "token": token_info,
        "recent_messages": [
            {
                "sent_at": m.sent_at.strftime("%d/%m %H:%M:%S") if m.sent_at else "",
                "sender_type": m.sender_type,
                "sender_name": m.sender_name or "?",
                "message_type": m.message_type,
                "text": (m.message_text or "")[:150],
                "chat_id": m.chat_id,
                "chat_label": labels.get(m.chat_id, m.chat_id[:12]),
                "direction": getattr(m, "direction", None) or "",
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
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Kommo Dispatcher</title>
<style>
  :root { --bg: #0f172a; --card: #1e293b; --border: #334155; --text: #e2e8f0; --muted: #94a3b8; --dim: #64748b; --accent: #38bdf8; --green: #22c55e; --red: #ef4444; --orange: #f59e0b; --purple: #a78bfa; }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); padding: 16px; font-size: 14px; }

  .header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 20px; flex-wrap: wrap; gap: 8px; }
  .header h1 { font-size: 1.4rem; color: var(--accent); }
  .header .meta { font-size: 0.75rem; color: var(--dim); }

  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin-bottom: 16px; }
  .card { background: var(--card); border-radius: 10px; padding: 14px; text-align: center; border: 1px solid var(--border); }
  .card .num { font-size: 1.8rem; font-weight: 700; color: var(--accent); line-height: 1.2; }
  .card .label { font-size: 0.7rem; color: var(--muted); margin-top: 2px; text-transform: uppercase; letter-spacing: 0.5px; }
  .card.green .num { color: var(--green); }
  .card.red .num { color: var(--red); }
  .card.orange .num { color: var(--orange); }
  .card.purple .num { color: var(--purple); }

  .panels { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
  @media (max-width: 900px) { .panels { grid-template-columns: 1fr; } }

  .panel { background: var(--card); border-radius: 10px; padding: 14px; border: 1px solid var(--border); }
  .panel h2 { font-size: 0.9rem; color: var(--muted); margin-bottom: 10px; display: flex; align-items: center; gap: 6px; }
  .panel h2 .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
  .dot-green { background: var(--green); }
  .dot-red { background: var(--red); }
  .dot-orange { background: var(--orange); }

  .kv { display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #1e293b22; font-size: 0.82rem; }
  .kv .k { color: var(--dim); }
  .kv .v { font-weight: 600; }

  .rate { display: flex; align-items: center; gap: 8px; margin-top: 6px; }
  .rate-bar { flex: 1; height: 8px; background: var(--border); border-radius: 4px; overflow: hidden; }
  .rate-fill { height: 100%; border-radius: 4px; transition: width 0.5s; }
  .rate-text { font-size: 0.8rem; color: var(--muted); min-width: 80px; text-align: right; }

  .section { background: var(--card); border-radius: 10px; padding: 14px; border: 1px solid var(--border); margin-bottom: 16px; }
  .section h2 { font-size: 0.9rem; color: var(--muted); margin-bottom: 10px; }

  table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
  th { text-align: left; padding: 5px 6px; color: var(--dim); font-weight: 600; border-bottom: 1px solid var(--border); font-size: 0.72rem; text-transform: uppercase; }
  td { padding: 5px 6px; border-bottom: 1px solid #1e293b55; }
  tr:hover { background: #ffffff08; }

  .badge { display: inline-block; padding: 1px 7px; border-radius: 8px; font-size: 0.7rem; font-weight: 600; }
  .b-contact { background: #065f46; color: #6ee7b7; }
  .b-user { background: #1e3a5f; color: #7dd3fc; }
  .b-bot { background: #4a1d6e; color: #c4b5fd; }
  .b-inbound { background: #065f46; color: #6ee7b7; }
  .b-outbound { background: #1e3a5f; color: #7dd3fc; }
  .b-voice { background: #7c2d12; color: #fdba74; }
  .b-picture { background: #365314; color: #bef264; }
  .b-file { background: #3b3b1a; color: #fde047; }
  .b-video { background: #4a1d6e; color: #c4b5fd; }
  .b-ok { background: #065f46; color: #6ee7b7; }
  .b-failed { background: #7f1d1d; color: #fca5a5; }

  .text-preview { max-width: 350px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--muted); }
  .empty { color: var(--dim); font-style: italic; text-align: center; padding: 20px; }
</style>
</head>
<body>

<div class="header">
  <h1>Kommo Dispatcher</h1>
  <div class="meta" id="meta">Carregando...</div>
</div>

<div class="grid" id="cards"></div>

<div class="panels">
  <div class="panel" id="panelN8n">
    <h2><span class="dot" id="n8nDot"></span> Fila n8n</h2>
    <div id="n8nInfo"></div>
  </div>
  <div class="panel" id="panelToken">
    <h2><span class="dot" id="tokenDot"></span> Token Amojo</h2>
    <div id="tokenInfo"></div>
  </div>
</div>

<div class="panel" style="margin-bottom:16px">
  <h2>Rate Limit Kommo</h2>
  <div class="rate">
    <div class="rate-bar"><div class="rate-fill" id="rateBar"></div></div>
    <div class="rate-text" id="rateText"></div>
  </div>
</div>

<div class="section" id="n8nRecent">
  <h2>Ultimos Dispatches n8n</h2>
  <table>
    <thead><tr><th>Hora</th><th>Evento</th><th>Lead</th><th>Tipo</th><th>HTTP</th><th>Status</th></tr></thead>
    <tbody id="dispatchTable"></tbody>
  </table>
</div>

<div class="section">
  <h2>Mensagens Recentes (SQLite local)</h2>
  <table>
    <thead><tr><th>Hora</th><th>Chat</th><th>Remetente</th><th>Dir</th><th>Tipo</th><th>Mensagem</th></tr></thead>
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
const B = (cls, text) => `<span class="badge b-${cls}">${text}</span>`;

function senderB(t) {
  if (t === 'bot') return B('bot', 'Bot');
  if (t === 'user') return B('user', 'Consultor');
  return B('contact', 'Lead');
}
function typeB(t) {
  if (t === 'voice') return B('voice', 'audio');
  if (t === 'picture') return B('picture', 'img');
  if (t === 'file') return B('file', 'arq');
  if (t === 'video') return B('video', 'video');
  return '';
}
function dirB(d) {
  if (d === 'inbound') return B('inbound', 'IN');
  if (d === 'outbound') return B('outbound', 'OUT');
  return '';
}
function fmtSec(s) {
  if (s <= 0) return 'expirado';
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

async function refresh() {
  try {
    const r = await fetch('/api/kommo/dashboard/stats');
    const d = await r.json();

    // Cards
    document.getElementById('cards').innerHTML = `
      <div class="card"><div class="num">${d.active_chats}</div><div class="label">Chats Monitorados</div></div>
      <div class="card green"><div class="num">${d.synced_chats}</div><div class="label">Chats Sincronizados</div></div>
      <div class="card orange"><div class="num">${d.pending_sync_chats}</div><div class="label">Aguardando Sync</div></div>
      <div class="card"><div class="num">${d.total_messages}</div><div class="label">Msgs Capturadas</div></div>
      <div class="card green"><div class="num">${d.n8n.sent_ok}</div><div class="label">Enviadas n8n</div></div>
      <div class="card ${d.n8n.failed > 0 ? 'red' : ''}""><div class="num">${d.n8n.failed}</div><div class="label">Falhas n8n</div></div>
      <div class="card purple"><div class="num">${d.n8n.pending}</div><div class="label">Na Fila</div></div>
      <div class="card"><div class="num">${d.by_type.voice || 0}</div><div class="label">Audios</div></div>
    `;

    // N8n panel
    const n = d.n8n;
    document.getElementById('n8nDot').className = 'dot ' + (n.pending > 10 ? 'dot-orange' : 'dot-green');
    document.getElementById('n8nInfo').innerHTML = `
      <div class="kv"><span class="k">Pendentes na fila</span><span class="v">${n.pending}</span></div>
      <div class="kv"><span class="k">Total enfileiradas</span><span class="v">${n.total_enqueued}</span></div>
      <div class="kv"><span class="k">Enviadas com sucesso</span><span class="v" style="color:var(--green)">${n.sent_ok}</span></div>
      <div class="kv"><span class="k">Falhas</span><span class="v" style="color:${n.failed ? 'var(--red)' : 'var(--text)'}">${n.failed}</span></div>
      <div class="kv"><span class="k">Webhook</span><span class="v" style="font-size:0.7rem;color:var(--dim)">${(n.webhook_url || '').replace('https://', '')}</span></div>
    `;

    // Token panel
    const t = d.token;
    const expired = t.is_expired;
    document.getElementById('tokenDot').className = 'dot ' + (expired ? 'dot-red' : 'dot-green');
    document.getElementById('tokenInfo').innerHTML = `
      <div class="kv"><span class="k">Status</span><span class="v" style="color:${expired ? 'var(--red)' : 'var(--green)'}">${expired ? 'EXPIRADO' : 'ATIVO'}</span></div>
      <div class="kv"><span class="k">Expira em</span><span class="v">${fmtSec(t.seconds_until_expiry)}</span></div>
      <div class="kv"><span class="k">Token</span><span class="v" style="font-size:0.7rem;color:var(--dim)">${t.token_preview}</span></div>
    `;

    // Rate limit
    const rl = d.rate_limit;
    const pct = (rl.requests_last_minute / rl.max_rpm) * 100;
    document.getElementById('rateBar').style.width = pct + '%';
    document.getElementById('rateBar').style.background = pct > 80 ? 'var(--red)' : pct > 50 ? 'var(--orange)' : 'var(--green)';
    document.getElementById('rateText').textContent = `${rl.requests_last_minute} / ${rl.max_rpm} rpm`;

    // Dispatch table
    const dispatches = n.recent || [];
    document.getElementById('dispatchTable').innerHTML = dispatches.length
      ? dispatches.map(d => `<tr>
          <td>${(d.ts||'').split('T')[1]?.substring(0,8) || ''}</td>
          <td>${d.event}</td>
          <td style="max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${d.lead}</td>
          <td>${typeB(d.type) || d.type}</td>
          <td>${d.http}</td>
          <td>${B(d.result === 'ok' ? 'ok' : 'failed', d.result)}</td>
        </tr>`).join('')
      : '<tr><td colspan="6" class="empty">Nenhum dispatch ainda</td></tr>';

    // Recent messages
    document.getElementById('recentTable').innerHTML = d.recent_messages.length
      ? d.recent_messages.map(m => `<tr>
          <td>${m.sent_at}</td>
          <td style="max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${m.chat_label}</td>
          <td>${senderB(m.sender_type)} ${m.sender_name}</td>
          <td>${dirB(m.direction)}</td>
          <td>${typeB(m.message_type)}</td>
          <td class="text-preview">${m.text || '[' + m.message_type + ']'}</td>
        </tr>`).join('')
      : '<tr><td colspan="6" class="empty">Nenhuma mensagem capturada</td></tr>';

    // Top chats
    document.getElementById('chatsTable').innerHTML = d.top_chats.length
      ? d.top_chats.map(c => `<tr>
          <td>${c.label}</td>
          <td>${c.msg_count}</td>
          <td>${c.last_at}</td>
        </tr>`).join('')
      : '<tr><td colspan="3" class="empty">Nenhum chat ativo</td></tr>';

    document.getElementById('meta').textContent =
      new Date().toLocaleTimeString('pt-BR') + ' | auto-refresh 5s';

  } catch(e) {
    document.getElementById('meta').textContent = 'Erro: ' + e.message;
  }
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"""
