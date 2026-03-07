from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class MonitoredChat(Base):
    __tablename__ = "monitored_chats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    label: Mapped[str | None] = mapped_column(String(512), nullable=True)
    lead_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    contact_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    last_message_uid: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_message_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    active: Mapped[bool] = mapped_column(default=True)

    lead_nome: Mapped[str | None] = mapped_column(String(512), nullable=True)
    contact_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    responsible_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pipeline_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    chat_source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_polled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
