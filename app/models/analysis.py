from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

AutoPK = Integer()


class AnalysisResultDB(Base):
    __tablename__ = "analysis_results"

    id: Mapped[int] = mapped_column(AutoPK, primary_key=True, autoincrement=True)

    chat_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    contact_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    atendimento_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    analyzed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )

    window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    message_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    media_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    transcript: Mapped[str | None] = mapped_column(Text, nullable=True)

    summary_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    sentiment: Mapped[str | None] = mapped_column(String(50), nullable=True)
    tabulation_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    consultor_responsavel: Mapped[str | None] = mapped_column(String(255), nullable=True)
    nota_atendimento: Mapped[float | None] = mapped_column(Float, nullable=True)

    supabase_synced: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
