"""
Renovacao automatica do amojo token via Playwright.

Faz login headless no Kommo, extrai o amojo_token do localStorage
e atualiza o token_manager em memoria + persiste no .env.
"""

import asyncio
import json
import logging
import re
import time as _time
from pathlib import Path

from app.config import get_settings
from app.services.token_manager import update_token

logger = logging.getLogger(__name__)

_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"

_RETRY_DELAY_SECONDS = 300
_MAX_RETRIES = 3
_LOGIN_TIMEOUT_MS = 60_000
_TOKEN_WAIT_TIMEOUT_MS = 30_000


def _is_configured() -> bool:
    s = get_settings()
    return bool(s.kommo_login_email and s.kommo_login_password)


async def renew_token_once() -> bool:
    """
    Executa um ciclo de renovacao: abre browser, faz login,
    extrai token, atualiza estado. Retorna True se bem-sucedido.
    """
    if not _is_configured():
        logger.warning("Token renewer: credenciais de login nao configuradas")
        return False

    settings = get_settings()
    login_url = settings.kommo_base_url

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error(
            "Playwright nao instalado. Execute: pip install playwright && playwright install chromium"
        )
        return False

    logger.info("Token renewer: iniciando login no Kommo...")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            context = await browser.new_context(
                viewport={"width": 1280, "height": 720},
                locale="pt-BR",
            )
            page = await context.new_page()

            await page.goto(login_url, wait_until="networkidle", timeout=_LOGIN_TIMEOUT_MS)
            logger.debug("Token renewer: pagina carregada — %s", page.url)

            filled = await _fill_login_form(page, settings.kommo_login_email, settings.kommo_login_password)
            if not filled:
                logger.error("Token renewer: nao encontrou formulario de login")
                return False

            await _wait_for_dashboard(page)
            logger.info("Token renewer: login OK — %s", page.url)

            token_data = await _extract_amojo_token(page)
            if not token_data:
                logger.error("Token renewer: amojo_token nao encontrado no localStorage")
                return False

            token = token_data.get("token", "")
            refresh_token = token_data.get("refreshToken", "")
            expired_at = token_data.get("expiredAt", 0)

            if not token:
                logger.error("Token renewer: campo 'token' vazio no amojo_token")
                return False

            update_token(token=token, refresh_token=refresh_token, expired_at=expired_at)
            _persist_to_env(token, refresh_token)

            await _save_session_cookies(context)

            logger.info(
                "Token renewer: token renovado com sucesso (expira em %ds)",
                max(0, expired_at - int(_time.time())),
            )
            return True

        except Exception:
            logger.exception("Token renewer: erro durante renovacao")
            return False
        finally:
            await browser.close()


async def _fill_login_form(page, email: str, password: str) -> bool:
    """Preenche o formulario de login do Kommo."""
    try:
        email_sel = 'input[name="login"], input[type="email"], input[id="session_end_login"]'
        await page.wait_for_selector(email_sel, timeout=15_000)
        await page.fill(email_sel, email)

        pass_sel = 'input[name="password"], input[type="password"], input[id="password"]'
        await page.wait_for_selector(pass_sel, timeout=5_000)
        await page.fill(pass_sel, password)

        submit_sel = 'button[type="submit"], input[type="submit"], button[id="auth_submit"]'
        submit = page.locator(submit_sel).first
        if await submit.count() > 0:
            await submit.click()
        else:
            await page.keyboard.press("Enter")

        return True
    except Exception:
        logger.exception("Token renewer: erro ao preencher formulario")
        return False


async def _wait_for_dashboard(page) -> None:
    """Aguarda a pagina redirecionar para o dashboard apos login."""
    try:
        await page.wait_for_url(
            re.compile(r"(leads|dashboard|pipeline|#)"),
            timeout=_LOGIN_TIMEOUT_MS,
        )
    except Exception:
        await asyncio.sleep(5)
        current = page.url
        if "login" in current.lower() or "auth" in current.lower():
            raise RuntimeError(f"Login falhou, ainda na pagina de auth: {current}")
        logger.debug("Token renewer: URL pos-login — %s", current)


async def _save_session_cookies(context) -> None:
    """Extrai cookies do browser context e salva no session_manager."""
    try:
        from app.services.session_manager import update_from_playwright_cookies
        pw_cookies = await context.cookies()
        update_from_playwright_cookies(pw_cookies)
        logger.info("Token renewer: %d cookies de sessao salvos", len(pw_cookies))
    except Exception:
        logger.exception("Token renewer: erro ao salvar cookies de sessao")


async def _extract_amojo_token(page) -> dict | None:
    """Extrai o amojo_token do localStorage do navegador."""
    for attempt in range(6):
        raw = await page.evaluate("() => localStorage.getItem('amojo_token')")
        if raw:
            try:
                data = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(data, dict) and data.get("token"):
                    return data
            except (json.JSONDecodeError, TypeError):
                pass
        await asyncio.sleep(3)

    return None


def _persist_to_env(token: str, refresh_token: str) -> None:
    """Atualiza KOMMO_AMOJO_TOKEN e KOMMO_AMOJO_REFRESH_TOKEN no .env."""
    if not _ENV_PATH.exists():
        return

    try:
        content = _ENV_PATH.read_text(encoding="utf-8")

        content = re.sub(
            r"^KOMMO_AMOJO_TOKEN=.*$",
            f"KOMMO_AMOJO_TOKEN={token}",
            content,
            flags=re.MULTILINE,
        )
        content = re.sub(
            r"^KOMMO_AMOJO_REFRESH_TOKEN=.*$",
            f"KOMMO_AMOJO_REFRESH_TOKEN={refresh_token}",
            content,
            flags=re.MULTILINE,
        )

        _ENV_PATH.write_text(content, encoding="utf-8")
        logger.debug("Token renewer: .env atualizado")
    except Exception:
        logger.exception("Token renewer: erro ao atualizar .env")


async def run_token_renewer() -> None:
    """Background loop que renova o token periodicamente."""
    settings = get_settings()
    interval = settings.token_renewal_interval_hours * 3600

    if not _is_configured():
        logger.warning(
            "Token renewer desabilitado: KOMMO_LOGIN_EMAIL / KOMMO_LOGIN_PASSWORD nao configurados"
        )
        return

    logger.info(
        "Token renewer iniciado (intervalo=%dh)",
        settings.token_renewal_interval_hours,
    )

    await asyncio.sleep(120)

    while True:
        success = False
        for retry in range(_MAX_RETRIES):
            try:
                success = await renew_token_once()
                if success:
                    break
            except Exception:
                logger.exception("Token renewer: tentativa %d/%d falhou", retry + 1, _MAX_RETRIES)

            if retry < _MAX_RETRIES - 1:
                logger.info("Token renewer: retry em %ds...", _RETRY_DELAY_SECONDS)
                await asyncio.sleep(_RETRY_DELAY_SECONDS)

        if not success:
            logger.error("Token renewer: todas as tentativas falharam neste ciclo")

        await asyncio.sleep(interval)
