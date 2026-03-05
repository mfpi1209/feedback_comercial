# Kommo Chat Sync

Captura mensagens de chat do Kommo (WhatsApp, Telegram, Instagram) e exporta em formato texto limpo para análise por IA.

## Arquitetura

```
Talks API (Bearer)  →  lista conversas (chat_id, lead_id, origin)
        ↓
Amojo API v1 (x-auth-token)  →  busca conteúdo das mensagens
        ↓
PostgreSQL (kommo_messages)  →  armazena tudo
        ↓
API REST (FastAPI)  →  consulta e exporta para IA
```

## Pré-requisitos

- Python 3.11+
- PostgreSQL
- Credenciais do Kommo:
  - **Access Token** (Bearer) — obtido na configuração da integração
  - **Amojo ID** — obtido de `GET /api/v4/account?with=amojo_id`
  - **Amojo Token** (x-auth-token) — token de sessão do amojo (ver instruções abaixo)

## Setup

```bash
# 1. Instalar dependências
pip install -r requirements.txt

# 2. Configurar variáveis de ambiente
cp .env.example .env
# Editar .env com suas credenciais

# 3. Rodar
uvicorn app.main:app --reload --port 8000
```

## Dois modos de captura

### Modo 1: Sync via Amojo API v1 (requer KOMMO_AMOJO_TOKEN)
Busca histórico completo de todas as conversas via endpoint interno do amojo.
Roda periodicamente em background (configurável via SYNC_INTERVAL_SECONDS).
Também permite sync direto por `chat_id` sem depender da Talks API.

### Modo 2: Webhook (complementar)
Configure a URL `https://seuservidor.com/api/kommo/webhook/messages` no Kommo.
Captura mensagens novas em tempo real para complementar o sync periódico.

## Variáveis de Ambiente

| Variável | Descrição |
|---|---|
| `DATABASE_URL` | Connection string PostgreSQL (asyncpg) |
| `KOMMO_BASE_URL` | URL base do Kommo (`https://admamoeduitcombr.kommo.com`) |
| `KOMMO_ACCESS_TOKEN` | Bearer token da API principal |
| `KOMMO_AMOJO_ID` | UUID do amojo da conta (de `GET /api/v4/account?with=amojo_id`) |
| `KOMMO_AMOJO_TOKEN` | Token x-auth-token para a API interna do amojo |
| `KOMMO_WEBHOOK_SECRET` | Secret para validar webhooks recebidos (opcional) |
| `SYNC_BATCH_SIZE` | Mensagens por página (padrão: 50) |
| `SYNC_INTERVAL_SECONDS` | Intervalo do sync automático (padrão: 300s) |

## Endpoints

### Consultar mensagens por lead
```
GET /api/kommo/messages/by-lead/{lead_id}?limit=100&offset=0&order=asc
```

### Consultar mensagens por chat_id
```
GET /api/kommo/messages/by-chat/{chat_id}?limit=100&offset=0&order=asc
```

### Exportar conversa de um lead para IA
```
GET /api/kommo/messages/export/{lead_id}
```

### Exportar conversa de um chat_id para IA
```
GET /api/kommo/messages/export-chat/{chat_id}
```

Retorna:
- `conversation`: lista estruturada (timestamp, sender, texto)
- `plain_text`: texto corrido pronto para prompt de IA

### Sync completo (todas as talks)
```
POST /api/kommo/messages/sync
```

### Sync por lead
```
POST /api/kommo/messages/sync/{lead_id}
```

### Sync direto por chat_id (sem Talks API)
```
POST /api/kommo/messages/sync-chat/{chat_id}?lead_id=0
```

### Webhook para mensagens em tempo real
```
POST /api/kommo/webhook/messages
```

### Health check
```
GET /health
```

## Onde encontrar as credenciais

1. **Access Token**: Configurações → Integrações → sua integração → Keys
2. **Amojo ID**: `GET /api/v4/account?with=amojo_id` → campo `amojo_id`
3. **Amojo Token (x-auth-token)**: Ver seção abaixo

### Como obter o x-auth-token

O `x-auth-token` é usado pela interface web do Kommo para acessar o amojo.
Para obtê-lo:

1. Abra o Kommo no navegador e faça login
2. Abra o DevTools (F12) → aba **Network**
3. Abra qualquer conversa de chat
4. Procure requisições para `amojo.kommo.com` (ex: `/v1/chats/.../messages`)
5. Copie o valor do header `x-auth-token` da requisição

> **Atenção**: Este token pode ter validade limitada. Monitore logs de erro 401
> para saber quando atualizar.

## Como funciona o endpoint de mensagens

```
GET https://amojo.kommo.com/v1/chats/{amojo_id}/{chat_id}/messages?with_video=true&stand=v16
Header: x-auth-token: {token}
```

Retorna array de mensagens com:
- `author.origin`: "waba" (contato), "amocrm" (consultor), "bot" (automação)
- `text`: conteúdo da mensagem
- `message.type`: text, picture, file, voice, video
- `created_at`: timestamp Unix
- `delivery_status`: 0 (recebida), 1 (enviada), 2 (lida)

## Tabela `kommo_messages`

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | BIGINT PK | ID auto-incremento |
| `lead_id` | BIGINT | ID do lead no Kommo |
| `contact_id` | BIGINT | ID do contato |
| `talk_id` | VARCHAR | ID da conversa (talk/dialog) |
| `chat_id` | VARCHAR | ID do chat (amojo) |
| `sender_name` | VARCHAR | Nome do remetente |
| `sender_phone` | VARCHAR | Telefone do remetente |
| `sender_type` | VARCHAR | "contact", "user" ou "bot" |
| `message_text` | TEXT | Conteúdo da mensagem |
| `message_type` | VARCHAR | text, picture, file, voice, video |
| `media_url` | TEXT | URL de mídia (se aplicável) |
| `sent_at` | TIMESTAMP TZ | Data/hora de envio |
| `origin` | VARCHAR | Canal (waba, telegram, instagram) |
| `synced_at` | TIMESTAMP TZ | Quando foi sincronizado |
| `message_uid` | VARCHAR UK | UID único da mensagem (dedup) |
