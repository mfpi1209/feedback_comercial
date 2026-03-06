from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///./data/kommo_chat.db"

    kommo_base_url: str = "https://admamoeduitcombr.kommo.com"
    kommo_access_token: str = ""

    # Amojo (chat) — endpoint v1 com x-auth-token
    kommo_amojo_id: str = "e29f43f0-ebca-413b-ab2f-8e806cec4998"
    kommo_amojo_token: str = ""
    kommo_amojo_refresh_token: str = ""

    kommo_webhook_secret: str = ""

    # Login Kommo — para renovacao automatica do amojo token via Playwright
    kommo_login_email: str = ""
    kommo_login_password: str = ""
    token_renewal_interval_hours: int = 20

    # IA — processamento de mídia e análise
    openai_api_key: str = ""
    google_gemini_api_key: str = ""

    # Supabase — gravação de feedback
    supabase_url: str = ""
    supabase_key: str = ""

    # n8n — webhook para processamento externo
    n8n_webhook_url: str = ""

    sync_batch_size: int = 50
    sync_interval_seconds: int = 300

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
