import logging
import os
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import get_settings

logger = logging.getLogger(__name__)

_settings = get_settings()

if _settings.database_url.startswith("sqlite"):
    db_path = _settings.database_url.split("///")[-1]
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

engine = create_async_engine(_settings.database_url, echo=False)

async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    async with async_session() as session:
        yield session


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await _migrate_monitored_chats()


async def _migrate_monitored_chats():
    """Adiciona colunas novas ao monitored_chats se não existirem (SQLite)."""
    new_cols = {
        "lead_nome": "TEXT",
        "contact_name": "TEXT",
        "responsible_user_id": "INTEGER",
        "pipeline_id": "INTEGER",
        "status_id": "INTEGER",
        "chat_source": "TEXT",
        "last_polled_at": "TIMESTAMP",
    }
    async with engine.begin() as conn:
        try:
            result = await conn.execute(text("PRAGMA table_info(monitored_chats)"))
            existing = {row[1] for row in result}
        except Exception:
            return

        for col, dtype in new_cols.items():
            if col not in existing:
                await conn.execute(
                    text(f"ALTER TABLE monitored_chats ADD COLUMN {col} {dtype}")
                )
                logger.info("Migração: coluna '%s' adicionada a monitored_chats", col)


def dialect_insert(table):
    """
    Retorna insert() compatível com on_conflict_do_nothing()
    para PostgreSQL e SQLite.
    """
    dialect = engine.dialect.name
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert
    return insert(table)
