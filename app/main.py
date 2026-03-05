import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from app.config import get_settings
from app.database import init_db
from app.routes.messages import router as messages_router
from app.routes.webhook import router as webhook_router
from app.routes.token import router as token_router
from app.routes.dashboard import router as dashboard_router
from app.routes.monitor import router as monitor_router
from app.routes.analysis import router as analysis_router
from app.routes.atendimento import router as atendimento_router
from app.services.chat_discovery import run_chat_discovery
from app.services.live_monitor import run_live_monitor
from app.services.atendimento_detector import run_atendimento_detector
from app.services.auto_analyzer import run_auto_analyzer
from app.services.token_manager import init_from_settings, get_current_token

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    init_from_settings()

    tasks = []

    if get_current_token():
        tasks.append(asyncio.create_task(run_live_monitor()))
        logger.info("Monitor ao vivo iniciado")
    else:
        logger.warning(
            "KOMMO_AMOJO_TOKEN não configurado — monitor desabilitado. "
            "Use PUT /api/kommo/token para configurar."
        )

    settings = get_settings()
    if settings.kommo_access_token:
        tasks.append(asyncio.create_task(run_chat_discovery()))
        logger.info("Discovery automático de chats iniciado")
    else:
        logger.warning(
            "KOMMO_ACCESS_TOKEN não configurado — discovery automático desabilitado. "
            "Adicione chats manualmente via POST /api/kommo/monitor/chats"
        )

    tasks.append(asyncio.create_task(run_atendimento_detector()))
    logger.info("Detector de atendimentos iniciado")

    tasks.append(asyncio.create_task(run_auto_analyzer()))
    logger.info("Auto-analyzer iniciado")

    yield

    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="Kommo Chat Sync",
    description="Captura e exporta mensagens de chat do Kommo para análise por IA",
    version="3.0.0",
    lifespan=lifespan,
)

app.include_router(dashboard_router)
app.include_router(monitor_router)
app.include_router(messages_router)
app.include_router(webhook_router)
app.include_router(token_router)
app.include_router(analysis_router)
app.include_router(atendimento_router)


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")


@app.get("/health")
async def health():
    return {"status": "ok"}
