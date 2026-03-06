"""
Find atendimentos that were analyzed with fewer messages than they currently have,
and reset them to 'fechado' for re-analysis.
"""
import sqlite3, sys
sys.stdout.reconfigure(encoding="utf-8")
conn = sqlite3.connect("kommo_chat.db")
c = conn.cursor()

print("=== ATENDIMENTOS ANALISADOS COM DADOS INCOMPLETOS ===\n")

c.execute("""
    SELECT a.id, a.contact_id, a.lead_nome, a.message_count, a.status,
           ar.id as analysis_id, LENGTH(ar.transcript) as transcript_len
    FROM atendimentos a
    LEFT JOIN analysis_results ar ON ar.atendimento_id = a.id
    WHERE a.status = 'analisado' AND ar.id IS NOT NULL
    ORDER BY a.id
""")

to_reset = []
for r in c.fetchall():
    atend_id, contact_id, nome, msg_count, status, analysis_id, transcript_len = r
    # Estimate: typical msg is ~80 chars in transcript, short convos are ~50
    # If transcript is much shorter than expected, it was likely incomplete
    print(f"  #{atend_id} {nome or '?'}: {msg_count} msgs, transcript={transcript_len} chars, analysis=#{analysis_id}")
    
    # Specifically check Jan Marques
    if atend_id == 773:
        print(f"    >>> JAN MARQUES — resetting para re-análise")
        to_reset.append(atend_id)

# Also find any where transcript only covers first part
c.execute("""
    SELECT a.id, a.message_count, ar.transcript
    FROM atendimentos a
    JOIN analysis_results ar ON ar.atendimento_id = a.id
    WHERE a.status = 'analisado' AND a.message_count > 10
""")
for r in c.fetchall():
    atend_id, msg_count, transcript = r
    if transcript:
        lines = [l for l in transcript.split('\n') if l.strip().startswith('[')]
        transcript_msgs = len(lines)
        if transcript_msgs < msg_count * 0.7:
            if atend_id not in to_reset:
                to_reset.append(atend_id)
                print(f"  #{atend_id}: transcript tem {transcript_msgs} msgs mas atendimento tem {msg_count} — resetting")

print(f"\n=== RESETANDO {len(to_reset)} ATENDIMENTOS ===")
for aid in to_reset:
    c.execute("UPDATE atendimentos SET status = 'fechado' WHERE id = ?", (aid,))
    c.execute("DELETE FROM analysis_results WHERE atendimento_id = ?", (aid,))
    print(f"  #{aid} → fechado (analysis_result removido)")

conn.commit()
print("\nFeito! O auto-analyzer vai re-analisar no próximo ciclo.")
conn.close()
