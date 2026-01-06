import sqlite3

conn = sqlite3.connect("viewer_nmp_logs.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS viewer_nmp_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    msisdn TEXT NOT NULL,
    searched_by TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    original TEXT,
    current TEXT,
    source TEXT
)
""")

conn.commit()
conn.close()

print("✅ viewer_nmp_logs table created successfully.")
