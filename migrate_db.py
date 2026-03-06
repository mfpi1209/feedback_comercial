"""One-time migration to add new columns and tables for atendimento support."""
import sqlite3
import sys

DB_PATH = "kommo_chat.db"

MIGRATIONS = [
    ("monitored_chats", "contact_id", "ALTER TABLE monitored_chats ADD COLUMN contact_id INTEGER"),
    ("kommo_messages", "contact_id", "ALTER TABLE kommo_messages ADD COLUMN contact_id INTEGER"),
    ("analysis_results", "contact_id", "ALTER TABLE analysis_results ADD COLUMN contact_id INTEGER"),
    ("analysis_results", "atendimento_id", "ALTER TABLE analysis_results ADD COLUMN atendimento_id INTEGER"),
]

CREATE_ATENDIMENTOS = """
CREATE TABLE IF NOT EXISTS atendimentos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL,
    lead_id INTEGER,
    lead_nome VARCHAR(512),
    lead_telefone VARCHAR(50),
    chat_ids_json TEXT NOT NULL DEFAULT '[]',
    session_start DATETIME NOT NULL,
    session_end DATETIME NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL DEFAULT 'aberto',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS ix_atendimentos_contact_id ON atendimentos (contact_id)",
    "CREATE INDEX IF NOT EXISTS ix_atendimentos_status ON atendimentos (status)",
    "CREATE INDEX IF NOT EXISTS ix_monitored_chats_contact_id ON monitored_chats (contact_id)",
    "CREATE INDEX IF NOT EXISTS ix_kommo_messages_contact_sent ON kommo_messages (contact_id, sent_at)",
    "CREATE INDEX IF NOT EXISTS ix_analysis_results_contact_id ON analysis_results (contact_id)",
    "CREATE INDEX IF NOT EXISTS ix_analysis_results_atendimento_id ON analysis_results (atendimento_id)",
]


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for table, column, sql in MIGRATIONS:
        cursor.execute(f"PRAGMA table_info({table})")
        cols = [row[1] for row in cursor.fetchall()]
        if column not in cols:
            print(f"  + Adding {table}.{column}")
            cursor.execute(sql)
        else:
            print(f"  = {table}.{column} already exists")

    print("  + Creating atendimentos table")
    cursor.execute(CREATE_ATENDIMENTOS)

    for idx_sql in CREATE_INDEXES:
        idx_name = idx_sql.split("IF NOT EXISTS ")[1].split(" ON")[0]
        print(f"  + Index {idx_name}")
        cursor.execute(idx_sql)

    conn.commit()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cursor.fetchall()]
    print(f"\nTabelas: {tables}")
    for t in tables:
        cursor.execute(f"SELECT COUNT(*) FROM [{t}]")
        print(f"  {t}: {cursor.fetchone()[0]} registros")

    conn.close()
    print("\nMigração concluída!")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
