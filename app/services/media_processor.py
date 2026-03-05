"""
Processamento de mídia: transcrição de áudio, descrição de imagem/vídeo, OCR de arquivos.

Usa OpenAI (Whisper, GPT-4o Vision, Files+GPT-4.1) e Google Gemini (vídeo).
"""

import io
import logging
import tempfile
from pathlib import Path

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

_DOWNLOAD_TIMEOUT = 60.0
_API_TIMEOUT = 120.0

AUDIO_EXTENSIONS = {".ogg", ".mp3", ".wav", ".m4a", ".oga", ".opus", ".webm", ".mp4"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".3gp"}


async def _download_media(url: str) -> tuple[bytes, str]:
    """Baixa o conteúdo binário de uma URL de mídia. Retorna (bytes, content_type)."""
    async with httpx.AsyncClient(timeout=_DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        ct = resp.headers.get("content-type", "application/octet-stream")
        return resp.content, ct


def _guess_extension(url: str, content_type: str) -> str:
    from urllib.parse import urlparse
    path = urlparse(url).path
    ext = Path(path).suffix.lower()
    if ext:
        return ext

    ct_map = {
        "audio/ogg": ".ogg",
        "audio/mpeg": ".mp3",
        "audio/mp4": ".m4a",
        "audio/wav": ".wav",
        "audio/webm": ".webm",
        "video/mp4": ".mp4",
        "video/webm": ".webm",
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "application/pdf": ".pdf",
    }
    for prefix, e in ct_map.items():
        if content_type.startswith(prefix):
            return e
    return ".bin"


# ---------------------------------------------------------------------------
# AUDIO → OpenAI Whisper
# ---------------------------------------------------------------------------

async def transcribe_audio(media_url: str) -> str:
    """Baixa áudio e transcreve com OpenAI Whisper. Retorna 'Audio: <texto>'."""
    settings = get_settings()
    if not settings.openai_api_key:
        return "[Audio: API key OpenAI não configurada]"

    try:
        data, ct = await _download_media(media_url)
        ext = _guess_extension(media_url, ct)
        if ext not in AUDIO_EXTENSIONS:
            ext = ".ogg"

        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
            tmp.write(data)
            tmp_path = tmp.name

        async with httpx.AsyncClient(timeout=_API_TIMEOUT) as client:
            with open(tmp_path, "rb") as f:
                resp = await client.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {settings.openai_api_key}"},
                    data={"model": "whisper-1"},
                    files={"file": (f"audio{ext}", f, f"audio/{ext.strip('.')}")},
                )
            resp.raise_for_status()
            text = resp.json().get("text", "")

        Path(tmp_path).unlink(missing_ok=True)
        logger.info("Audio transcrito: %d chars", len(text))
        return f"Audio: {text}"

    except Exception as e:
        logger.error("Erro ao transcrever áudio %s: %s", media_url[:80], e)
        return "[Audio: erro na transcrição]"


# ---------------------------------------------------------------------------
# IMAGE → OpenAI GPT-4o Vision
# ---------------------------------------------------------------------------

async def describe_image(media_url: str) -> str:
    """Envia URL da imagem para GPT-4o Vision. Retorna 'Imagem: <descrição>'."""
    settings = get_settings()
    if not settings.openai_api_key:
        return "[Imagem: API key OpenAI não configurada]"

    try:
        payload = {
            "model": "gpt-4o",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": media_url}},
                        {"type": "text", "text": "Resuma o que está na imagem de forma objetiva."},
                    ],
                }
            ],
            "max_tokens": 500,
        }

        async with httpx.AsyncClient(timeout=_API_TIMEOUT) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.openai_api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]

        logger.info("Imagem descrita: %d chars", len(text))
        return f"Imagem: {text}"

    except Exception as e:
        logger.error("Erro ao descrever imagem %s: %s", media_url[:80], e)
        return "[Imagem: erro na descrição]"


# ---------------------------------------------------------------------------
# VIDEO → Google Gemini
# ---------------------------------------------------------------------------

async def describe_video(media_url: str) -> str:
    """Envia URL do vídeo para Gemini 2.5 Flash. Retorna 'Video: <descrição>'."""
    settings = get_settings()
    if not settings.google_gemini_api_key:
        return "[Video: API key Gemini não configurada]"

    try:
        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "file_data": {
                                "mime_type": "video/mp4",
                                "file_uri": media_url,
                            }
                        },
                        {"text": "Descreva sobre o que se trata o vídeo de forma objetiva."},
                    ]
                }
            ]
        }

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.0-flash:generateContent?key={settings.google_gemini_api_key}"
        )

        async with httpx.AsyncClient(timeout=_API_TIMEOUT) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            text = (
                data.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
            )

        logger.info("Video descrito: %d chars", len(text))
        return f"Video: {text}"

    except Exception as e:
        logger.error("Erro ao descrever vídeo %s: %s", media_url[:80], e)
        return "[Video: erro na descrição]"


# ---------------------------------------------------------------------------
# FILE/PDF → OpenAI Files API + GPT-4.1 Responses
# ---------------------------------------------------------------------------

async def transcribe_file(media_url: str) -> str:
    """Baixa arquivo, faz upload no OpenAI Files e transcreve com GPT-4.1."""
    settings = get_settings()
    if not settings.openai_api_key:
        return "[Arquivo: API key OpenAI não configurada]"

    try:
        data, ct = await _download_media(media_url)
        ext = _guess_extension(media_url, ct)
        filename = f"document{ext}"

        headers = {"Authorization": f"Bearer {settings.openai_api_key}"}

        async with httpx.AsyncClient(timeout=_API_TIMEOUT) as client:
            upload_resp = await client.post(
                "https://api.openai.com/v1/files",
                headers=headers,
                data={"purpose": "assistants"},
                files={"file": (filename, io.BytesIO(data), ct)},
            )
            upload_resp.raise_for_status()
            file_id = upload_resp.json()["id"]

            resp = await client.post(
                "https://api.openai.com/v1/responses",
                headers={**headers, "Content-Type": "application/json"},
                json={
                    "model": "gpt-4.1",
                    "input": [
                        {
                            "role": "user",
                            "content": [
                                {"type": "input_file", "file_id": file_id},
                                {
                                    "type": "input_text",
                                    "text": "Transcreva o conteúdo do arquivo de forma objetiva.",
                                },
                            ],
                        }
                    ],
                },
            )
            resp.raise_for_status()
            result = resp.json()
            text = result.get("output", [{}])[0].get("content", [{}])[0].get("text", "")

        logger.info("Arquivo transcrito: %d chars", len(text))
        return f"Arquivo: {text}"

    except Exception as e:
        logger.error("Erro ao transcrever arquivo %s: %s", media_url[:80], e)
        return "[Arquivo: erro na transcrição]"


# ---------------------------------------------------------------------------
# Dispatcher principal
# ---------------------------------------------------------------------------

async def process_media(message_type: str, media_url: str) -> str:
    """
    Processa uma mídia de acordo com seu tipo.
    Retorna texto enriquecido (transcrição/descrição) ou fallback.
    """
    if not media_url:
        return f"[{message_type}: sem URL]"

    msg_type = message_type.lower()

    if msg_type in ("voice", "audio"):
        return await transcribe_audio(media_url)
    elif msg_type in ("picture", "image"):
        return await describe_image(media_url)
    elif msg_type == "video":
        return await describe_video(media_url)
    elif msg_type in ("file", "document"):
        return await transcribe_file(media_url)
    else:
        return f"[Mídia tipo '{message_type}']"
