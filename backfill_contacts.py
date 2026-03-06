"""Backfill contact_id on kommo_messages from monitored_chats."""
import sqlite3
import sys

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = "kommo_chat.db"

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("""
    SELECT chat_id, contact_id
    FROM monitored_chats
    WHERE contact_id IS NOT NULL AND contact_id != 0
""")
chat_contacts = c.fetchall()
print(f"Chats com contact_id: {len(chat_contacts)}")

total_updated = 0
for chat_id, contact_id in chat_contacts:
    c.execute("""
        UPDATE kommo_messages
        SET contact_id = ?
        WHERE chat_id = ? AND (contact_id IS NULL OR contact_id = 0)
    """, (contact_id, chat_id))
    if c.rowcount > 0:
        total_updated += c.rowcount
        print(f"  Chat {chat_id[:12]}... -> contact {contact_id}: {c.rowcount} msgs")

conn.commit()

c.execute("SELECT COUNT(*) FROM kommo_messages WHERE contact_id IS NOT NULL AND contact_id != 0")
with_contact = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM kommo_messages")
total = c.fetchone()[0]
print(f"\nResultado: {with_contact}/{total} mensagens com contact_id ({100*with_contact//total}%)")
print(f"Total atualizado: {total_updated}")

c.execute("SELECT COUNT(DISTINCT contact_id) FROM kommo_messages WHERE contact_id IS NOT NULL AND contact_id != 0")
print(f"Contatos distintos: {c.fetchone()[0]}")

conn.close()
