import sqlite3
from datetime import datetime, timedelta
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "logs", "trace.db")
LOG_FILE = os.path.join(os.path.dirname(__file__), "..", "logs", "prune.log")

cutoff = (datetime.now() - timedelta(days=30)).strftime("%b %d %Y %H:%M:%S GMT-04")

with open(LOG_FILE, "a") as log:
    log.write(f"🧹 [{datetime.now()}] Pruning traces older than {cutoff}\n")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("DELETE FROM radius_matches WHERE timestamp < ?", (cutoff,))
deleted = cursor.rowcount
conn.commit()
conn.close()

with open(LOG_FILE, "a") as log:
    log.write(f"✅ Deleted {deleted} old trace records.\n")
