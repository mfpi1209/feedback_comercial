from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

AutoPK = BigInteger().with_variant(Integer, "sqlite")


class KommoMessage(Base):
    __tablename__ = "kommo_messages"

    id: Mapped[int] = mapped_column(AutoPK, primary_key=True, autoincrement=True)
    lead_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    contact_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    talk_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    chat_id: Mapped[str] = mapped_column(String(255), nullable=False)
    sender_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    sender_phone: Mapped[str | None] = mapped_column(String(50), nullable=True)
    sender_type: Mapped[str] = mapped_column(String(50), nullable=False)  # "contact" | "user"
    message_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_type: Mapped[str] = mapped_column(String(50), nullable=False, default="text")  # text, picture, file, voice, video
    media_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    origin: Mapped[str | None] = mapped_column(String(50), nullable=True)  # whatsapp, telegram, instagram
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    message_uid: Mapped[str | None] = mapped_column(String(255), nullable=True, unique=True)

    __table_args__ = (
        Index("ix_kommo_messages_lead_sent", "lead_id", "sent_at"),
        Index("ix_kommo_messages_chat_sent", "chat_id", "sent_at"),
    )
