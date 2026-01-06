import sqlite3

conn = sqlite3.connect("logs/trace.db")
c = conn.cursor()

c.execute("""
CREATE TABLE IF NOT EXISTS radius_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    msisdn TEXT,
    imsi TEXT,
    enodeb_id INTEGER,
    cell_id INTEGER,
    tower_name TEXT,
    lat REAL,
    lon REAL,
    timestamp TEXT
)
""")

conn.commit()
conn.close()
print("✅ trace_db.sqlite initialized.")
