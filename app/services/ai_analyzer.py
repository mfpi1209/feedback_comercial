"""
Pipeline de análise IA de conversas comerciais.

Fase 1 — Resumo (Google Gemini Flash)
Fase 2 — Análise de Sentimento (OpenAI)
Fase 3 — Tabulação Comercial (OpenAI GPT-4o-mini)
"""

import json
import logging
from dataclasses import dataclass, field

import httpx

from app.config import get_settings
from app.services.conversation_builder import ConversationData

logger = logging.getLogger(__name__)

_API_TIMEOUT = 120.0


def _safe_parse_json(text: str) -> dict | None:
    """Tenta extrair JSON de uma resposta que pode conter markdown ou texto extra."""
    text = text.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    for start_char, end_char in [("{", "}"), ("[", "]")]:
        a = text.find(start_char)
        b = text.rfind(end_char)
        if a >= 0 and b > a:
            try:
                return json.loads(text[a : b + 1])
            except json.JSONDecodeError:
                pass
    return None


# ---------------------------------------------------------------------------
# Fase 1 — Resumo (Google Gemini Flash)
# ---------------------------------------------------------------------------

SUMMARY_SYSTEM_PROMPT = """Você recebe o histórico de um atendimento COMERCIAL entre um lead (potencial cliente) e consultores de vendas.
Sua tarefa é RESUMIR o atendimento de forma objetiva, mantendo:
- O interesse/necessidade principal do lead
- O nome do consultor responsável (passe somente o nome de um — o último consultor humano)
- O que o consultor propôs/orientou
- Se o lead demonstrou interesse em avançar ou não
- Qualquer detalhe importante (produto mencionado, objeção, prazo, etc.)

Responda SEMPRE em JSON com o formato:
{
  "resumo": "...",
  "interesse_lead": "...",
  "acao_consultor": "...",
  "lead_avancou": true/false,
  "observacoes": "..."
}"""


async def phase_summarize(conversation: ConversationData) -> dict:
    """Fase 1: Resumo da conversa via Google Gemini."""
    settings = get_settings()

    if settings.google_gemini_api_key:
        return await _summarize_gemini(conversation, settings)
    elif settings.openai_api_key:
        return await _summarize_openai(conversation, settings)
    else:
        logger.warning("Nenhuma API key configurada para resumo")
        return {"resumo": "API key não configurada", "interesse_lead": "", "acao_consultor": "", "lead_avancou": False, "observacoes": ""}


async def _summarize_gemini(conversation: ConversationData, settings) -> dict:
    try:
        payload = {
            "system_instruction": {"parts": [{"text": SUMMARY_SYSTEM_PROMPT}]},
            "contents": [
                {
                    "parts": [
                        {
                            "text": f"Segue o histórico do atendimento comercial para você resumir:\n\n{conversation.transcript}"
                        }
                    ]
                }
            ],
            "generationConfig": {"responseMimeType": "application/json"},
        }

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.0-flash-lite:generateContent?key={settings.google_gemini_api_key}"
        )

        async with httpx.AsyncClient(timeout=_API_TIMEOUT) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            text = (
                data.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "{}")
            )

        result = _safe_parse_json(text) or {}
        logger.info("Resumo Gemini gerado: %s", result.get("resumo", "")[:80])
        return result

    except Exception as e:
        logger.error("Erro no resumo Gemini: %s", e)
        return {"resumo": f"Erro: {e}", "interesse_lead": "", "acao_consultor": "", "lead_avancou": False, "observacoes": ""}


async def _summarize_openai(conversation: ConversationData, settings) -> dict:
    try:
        async with httpx.AsyncClient(timeout=_API_TIMEOUT) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.openai_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4.1-nano",
                    "messages": [
                        {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
                        {"role": "user", "content": f"Segue o histórico:\n\n{conversation.transcript}"},
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.2,
                },
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]

        result = _safe_parse_json(text) or {}
        logger.info("Resumo OpenAI gerado: %s", result.get("resumo", "")[:80])
        return result

    except Exception as e:
        logger.error("Erro no resumo OpenAI: %s", e)
        return {"resumo": f"Erro: {e}", "interesse_lead": "", "acao_consultor": "", "lead_avancou": False, "observacoes": ""}


# ---------------------------------------------------------------------------
# Fase 2 — Análise de Sentimento (OpenAI)
# ---------------------------------------------------------------------------

SENTIMENT_SYSTEM_PROMPT = """Você é um analisador de sentimento altamente inteligente e preciso.
Analise o sentimento do texto fornecido — que é uma conversa entre um lead e um consultor comercial.
Classifique o sentimento GERAL do lead em uma das seguintes categorias: Positive, Negative, Neutral.

Responda SOMENTE em JSON com o formato:
{"sentiment": "Positive"} ou {"sentiment": "Negative"} ou {"sentiment": "Neutral"}"""


async def phase_sentiment(conversation: ConversationData) -> str:
    """Fase 2: Análise de sentimento via OpenAI."""
    settings = get_settings()
    if not settings.openai_api_key:
        if settings.google_gemini_api_key:
            return await _sentiment_gemini(conversation, settings)
        return "Neutral"

    try:
        async with httpx.AsyncClient(timeout=_API_TIMEOUT) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.openai_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4.1-nano",
                    "messages": [
                        {"role": "system", "content": SENTIMENT_SYSTEM_PROMPT},
                        {"role": "user", "content": conversation.transcript},
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0,
                },
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]
            result = _safe_parse_json(text) or {}
            sentiment = result.get("sentiment", "Neutral")

        logger.info("Sentimento: %s", sentiment)
        return sentiment

    except Exception as e:
        logger.error("Erro na análise de sentimento: %s", e)
        return "Neutral"


async def _sentiment_gemini(conversation: ConversationData, settings) -> str:
    try:
        payload = {
            "system_instruction": {"parts": [{"text": SENTIMENT_SYSTEM_PROMPT}]},
            "contents": [{"parts": [{"text": conversation.transcript}]}],
            "generationConfig": {"responseMimeType": "application/json"},
        }
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.0-flash-lite:generateContent?key={settings.google_gemini_api_key}"
        )
        async with httpx.AsyncClient(timeout=_API_TIMEOUT) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            result = _safe_parse_json(text) or {}
        return result.get("sentiment", "Neutral")
    except Exception as e:
        logger.error("Erro sentimento Gemini: %s", e)
        return "Neutral"


# ---------------------------------------------------------------------------
# Fase 3 — Tabulação Comercial (OpenAI GPT-4o-mini)
# ---------------------------------------------------------------------------

TABULATION_SYSTEM_PROMPT = """# Prompt — TABULAÇÃO de Qualidade de Atendimento Comercial

## IDENTIDADE E CONTEXTO
Você é um **Analista Sênior de Qualidade Comercial**.
Seu papel é **TABULAR** a interação de atendimento comercial de forma objetiva em UMA ÚNICA RESPOSTA.

## DADOS QUE VOCÊ RECEBERÁ

### 1. JSON com resumo da conversa (Fase 1):
Contém: resumo, interesse_lead, acao_consultor, lead_avancou, observacoes

### 2. Conversa completa (LOG):
Mensagens com timestamps no formato:
- `[2026-01-19T21:53:13] CLIENTE (João): Boa noite`
- `[2026-01-19T21:53:16] BOT (Bot): Olá!`
- `[2026-01-19T21:53:17] CONSULTOR (Maria): Olá, boa noite!`

### 3. Sentimento identificado:
"Positive", "Negative" ou "Neutral"

## ANÁLISE OBRIGATÓRIA

### IDENTIFICAÇÃO DO CONSULTOR RESPONSÁVEL
- Considere apenas consultores humanos (CONSULTOR, não BOT).
- Se houver um consultor, ele é o responsável.
- Se houver dois ou mais, use o ÚLTIMO consultor do log.
- Se não houver nenhum, use "Nao informado".

### CÁLCULO DA DURAÇÃO DO ATENDIMENTO
- INÍCIO: última mensagem do CLIENTE antes da primeira resposta do consultor_responsavel
- FIM: última mensagem do log
- Classifique: Até 5 min → "Muito curto", >5 até 15 → "Curto", >15 até 45 → "Medio", >45 → "Longo"
- Preencha tempo_atendimento: "X minutos"

### TEMPO MÉDIO DE RESPOSTA
- IGNORE mensagens de BOT
- Identifique cada par: mensagem do CLIENTE → resposta do CONSULTOR
- Calcule intervalo e a média
- Formate: "X minutos"

### TEMPO DE RESPOSTA ADEQUADO
- Se QUALQUER intervalo >= 30 min → false
- Se TODOS < 30 min → true

### QUALIDADE DA ABORDAGEM COMERCIAL
Avalie:
- O consultor fez perguntas de qualificação? (entendeu a necessidade)
- Apresentou produtos/serviços relevantes?
- Tratou objeções adequadamente?
- Propôs próximos passos claros?
- Manteve comunicação profissional e engajadora?

### INTERESSE DO LEAD
- "alto": demonstrou vontade clara de comprar/contratar
- "medio": mostrou curiosidade mas sem compromisso
- "baixo": não demonstrou interesse real

### STATUS DO LEAD
- "qualificado": tem perfil e interesse para avançar
- "em_negociacao": está discutindo condições
- "nao_qualificado": sem perfil ou interesse
- "perdido": desistiu ou não respondeu

### CÁLCULO DA NOTA (0 a 10)

**Se conseguiu calcular tempos (CENÁRIO A):**
- Tempo de Resposta Adequado: 1.5 pts (true=1.5, false=0, null=0.75)
- Qualidade da Abordagem: 2.5 pts (boa=2.5, regular=1.25, ruim=0)
- Engajamento do Lead: 2 pts (alto=2, medio=1, baixo=0)
- Sentimento: 1.5 pts (Positive=1.5, Neutral=0.75, Negative=0)
- Tratamento de Objeções: 1 pt (tratou=1, parcial=0.5, nenhum=0)
- Próximos Passos: 1 pt (definiu=1, vago=0.5, nenhum=0)
- Comunicação: 0.5 pts (profissional=0.5, regular=0.25, ruim=0)
- TOTAL: 10 pontos

**Se NÃO conseguiu calcular tempos (CENÁRIO B):**
- Qualidade da Abordagem: 3.5 pts
- Engajamento do Lead: 2.5 pts
- Sentimento: 1.5 pts
- Tratamento de Objeções: 1 pt
- Próximos Passos: 1 pt
- Comunicação: 0.5 pts
- TOTAL: 10 pontos

### CATEGORIZAÇÃO (nivel_1, nivel_2, nivel_3)
Classifique o atendimento em uma hierarquia de 3 níveis baseado no tema principal:
Exemplo: "Comercial > Curso de Graduação > Preço"
- nivel_1: área principal (Comercial, Financeiro, Informações, Pós-venda)
- nivel_2: subcategoria (ex: Curso de Graduação, MBA, Pós-graduação, EAD)
- nivel_3: assunto específico (ex: Preço, Matrícula, Grade, Bolsa, Prazo)

## OUTPUT FINAL

Retorne APENAS este JSON:

```json
{
  "consultor_responsavel": "[nome ou 'Nao informado']",
  "duracao_atendimento": "[Muito curto/Curto/Medio/Longo/Nao informado]",
  "tempo_atendimento": "[ex.: '12 minutos' ou 'Nao informado']",
  "tempo_medio_resposta": "[ex.: '3 minutos' ou 'Nao informado']",
  "ponto_positivo": "[até 120 caracteres]",
  "ponto_negativo": "[até 120 caracteres]",
  "feedback": {
    "demanda": "[até 120 caracteres - o que o lead procurava]",
    "proposta": "[o que o consultor ofereceu/propôs]",
    "objecoes": "[objeções levantadas pelo lead, se houver]",
    "resultado": "[como terminou a conversa]"
  },
  "tabela_niveis": {
    "nivel_1": "[categoria]",
    "nivel_2": "[subcategoria]",
    "nivel_3": "[assunto]"
  },
  "metricas_qualidade": {
    "tempo_resposta_adequado": true/false/null,
    "qualidade_abordagem": "[boa/regular/ruim]",
    "interesse_lead": "[alto/medio/baixo]",
    "status_lead": "[qualificado/em_negociacao/nao_qualificado/perdido]",
    "lead_engajado": true/false/null,
    "sentimento_categoria": "Positive/Negative/Neutral",
    "nota_atendimento": 8.5
  }
}
```

## REGRAS FINAIS
- Completar análise em UMA resposta
- Fazer TODOS os cálculos mentalmente
- Retornar JSON COMPLETO
- Seguir formato JSON rigorosamente
- NÃO adicionar texto fora do JSON"""


async def phase_tabulate(
    conversation: ConversationData,
    summary: dict,
    sentiment: str,
) -> dict:
    """Fase 3: Tabulação comercial completa via OpenAI GPT-4o-mini."""
    settings = get_settings()

    if settings.openai_api_key:
        return await _tabulate_openai(conversation, summary, sentiment, settings)
    elif settings.google_gemini_api_key:
        return await _tabulate_gemini(conversation, summary, sentiment, settings)
    else:
        logger.warning("Nenhuma API key para tabulação")
        return {}


def _build_tabulation_user_prompt(
    conversation: ConversationData,
    summary: dict,
    sentiment: str,
) -> str:
    return (
        f"## Resumo da Conversa (Fase 1):\n"
        f"```json\n{json.dumps(summary, ensure_ascii=False, indent=2)}\n```\n\n"
        f"## Sentimento identificado: {sentiment}\n\n"
        f"## Conversa completa (LOG):\n{conversation.transcript}"
    )


async def _tabulate_openai(
    conversation: ConversationData,
    summary: dict,
    sentiment: str,
    settings,
) -> dict:
    try:
        user_prompt = _build_tabulation_user_prompt(conversation, summary, sentiment)

        async with httpx.AsyncClient(timeout=_API_TIMEOUT) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.openai_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": TABULATION_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.2,
                },
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]

        result = _safe_parse_json(text) or {}
        logger.info(
            "Tabulação OpenAI: consultor=%s, nota=%s",
            result.get("consultor_responsavel"),
            result.get("metricas_qualidade", {}).get("nota_atendimento"),
        )
        return result

    except Exception as e:
        logger.error("Erro na tabulação OpenAI: %s", e)
        return {"error": str(e)}


async def _tabulate_gemini(
    conversation: ConversationData,
    summary: dict,
    sentiment: str,
    settings,
) -> dict:
    try:
        user_prompt = _build_tabulation_user_prompt(conversation, summary, sentiment)

        payload = {
            "system_instruction": {"parts": [{"text": TABULATION_SYSTEM_PROMPT}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "generationConfig": {"responseMimeType": "application/json"},
        }
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.0-flash:generateContent?key={settings.google_gemini_api_key}"
        )

        async with httpx.AsyncClient(timeout=_API_TIMEOUT) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]

        result = _safe_parse_json(text) or {}
        logger.info(
            "Tabulação Gemini: consultor=%s, nota=%s",
            result.get("consultor_responsavel"),
            result.get("metricas_qualidade", {}).get("nota_atendimento"),
        )
        return result

    except Exception as e:
        logger.error("Erro na tabulação Gemini: %s", e)
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Pipeline completo
# ---------------------------------------------------------------------------

@dataclass
class AnalysisResult:
    chat_id: str
    message_count: int = 0
    media_count: int = 0
    window_start: str = ""
    window_end: str = ""
    transcript: str = ""
    summary: dict = field(default_factory=dict)
    sentiment: str = "Neutral"
    tabulation: dict = field(default_factory=dict)
    consultor_responsavel: str = ""
    nota_atendimento: float | None = None


async def run_full_analysis(conversation: ConversationData) -> AnalysisResult:
    """Executa as 3 fases de análise IA em sequência."""
    result = AnalysisResult(
        chat_id=conversation.chat_id,
        message_count=conversation.message_count,
        media_count=conversation.media_count,
        window_start=str(conversation.window_start or ""),
        window_end=str(conversation.window_end or ""),
        transcript=conversation.transcript,
    )

    if conversation.message_count == 0:
        logger.warning("Nenhuma mensagem para analisar em %s", conversation.chat_id)
        return result

    logger.info("=== Fase 1/3: Resumo — chat %s ===", conversation.chat_id)
    result.summary = await phase_summarize(conversation)

    logger.info("=== Fase 2/3: Sentimento — chat %s ===", conversation.chat_id)
    result.sentiment = await phase_sentiment(conversation)

    logger.info("=== Fase 3/3: Tabulação — chat %s ===", conversation.chat_id)
    result.tabulation = await phase_tabulate(conversation, result.summary, result.sentiment)

    result.consultor_responsavel = result.tabulation.get("consultor_responsavel", "")
    metricas = result.tabulation.get("metricas_qualidade", {})
    nota = metricas.get("nota_atendimento")
    if nota is not None:
        try:
            result.nota_atendimento = float(nota)
        except (ValueError, TypeError):
            pass

    logger.info(
        "Análise completa chat %s: consultor=%s, nota=%s, sentimento=%s",
        conversation.chat_id,
        result.consultor_responsavel,
        result.nota_atendimento,
        result.sentiment,
    )
    return result
