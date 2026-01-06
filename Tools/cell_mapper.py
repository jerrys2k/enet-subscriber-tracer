#!/usr/bin/env python3
"""
RADIUS Log Watcher
- Real-time monitoring of RADIUS logs
- PostgreSQL integration
- Enhanced logging with full tower location decoding
"""

import os
import re
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict

import psycopg2
from dotenv import load_dotenv

# Ensure Tools/ is importable
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from Tools.cell_mapper import decode_location_info

# ====================== CONFIG ======================
class Config:
    DB_CONFIG = {
        "dbname": "tracedb",
        "user": "enet",
        "password": "${DB_PASSWORD}",
        "host": "localhost",
        "port": "5432"
    }
    LOG_DIR = "/var/log/freeradius/radacct"
    LOG_FILE = "logs/radius_watcher.log"
    MAX_RETRIES = 3
    RETRY_DELAY = 1
    LOG_EVERY = 100

# ====================== LOGGING ======================
class LogFormatter(logging.Formatter):
    grey = "\x1b[38;21m"
    blue = "\x1b[38;5;39m"
    yellow = "\x1b[38;5;226m"
    red = "\x1b[38;5;196m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    def __init__(self, fmt):
        super().__init__()
        self.fmt = fmt
        self.FORMATS = {
            logging.DEBUG: self.grey + self.fmt + self.reset,
            logging.INFO: self.blue + self.fmt + self.reset,
            logging.WARNING: self.yellow + self.fmt + self.reset,
            logging.ERROR: self.red + self.fmt + self.reset,
            logging.CRITICAL: self.bold_red + self.fmt + self.reset
        }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    formatter = LogFormatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(Config.LOG_FILE)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    return logging.getLogger(__name__)

logger = setup_logging()

# ====================== DATABASE ======================
def get_db_connection():
    for attempt in range(Config.MAX_RETRIES):
        try:
            return psycopg2.connect(**Config.DB_CONFIG)
        except Exception as e:
            logger.warning(f"DB connect failed (attempt {attempt + 1}): {e}")
            time.sleep(Config.RETRY_DELAY)
    raise ConnectionError("Failed to connect to database")

def create_tables_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS radius_matches (
                id SERIAL PRIMARY KEY,
                msisdn VARCHAR(15),
                imsi VARCHAR(15),
                enodeb_id VARCHAR(20),
                cell_id VARCHAR(10),
                tower_name TEXT,
                lat FLOAT,
                lon FLOAT,
                timestamp TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT unique_radius_entry UNIQUE (msisdn, enodeb_id, cell_id, timestamp)
            );
            CREATE TABLE IF NOT EXISTS latest_traces (
                msisdn VARCHAR(15) PRIMARY KEY,
                imsi VARCHAR(15),
                enodeb_id VARCHAR(20),
                cell_id VARCHAR(10),
                tower_name TEXT,
                lat FLOAT,
                lon FLOAT,
                timestamp TIMESTAMPTZ,
                source VARCHAR(20),
                device_model TEXT,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()

def insert_record(msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO radius_matches (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT unique_radius_entry DO NOTHING;
                """, (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp))

                cur.execute("""
                    INSERT INTO latest_traces (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'watcher')
                    ON CONFLICT (msisdn) DO UPDATE SET
                        imsi = EXCLUDED.imsi,
                        enodeb_id = EXCLUDED.enodeb_id,
                        cell_id = EXCLUDED.cell_id,
                        tower_name = EXCLUDED.tower_name,
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        timestamp = EXCLUDED.timestamp,
                        source = 'watcher',
                        updated_at = NOW();
                """, (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp))
                conn.commit()
                return True
    except Exception as e:
        logger.error(f"Insert failed: {e}")
        return False

# ====================== LOG MONITOR ======================
def get_latest_log():
    try:
        logs = [os.path.join(dp, f) for dp, _, files in os.walk(Config.LOG_DIR) for f in files if f.startswith("detail-")]
        return max(logs, key=os.path.getmtime) if logs else None
    except Exception as e:
        logger.error(f"Failed to find logs: {e}")
        return None

def process_entry(entry: Dict[str, str]) -> bool:
    try:
        msisdn = entry.get("Calling-Station-Id", "").strip()
        imsi = entry.get("3GPP-IMSI", entry.get("User-Name", "")).strip()
        raw_loc = entry.get("3GPP-User-Location-Info", "").strip()
        raw_ts = entry.get("Event-Timestamp", "").strip()

        if not all([msisdn, raw_loc, raw_ts]):
            return False

        try:
            dt = datetime.strptime(raw_ts, "%b %d %Y %H:%M:%S %Z") if "UTC" in raw_ts else datetime.utcfromtimestamp(int(raw_ts))
            timestamp = dt.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.warning(f"Invalid timestamp {raw_ts}: {e}")
            return False

        tower, enodeb_id, cell_id, lat, lon = decode_location_info(raw_loc)
        logger.info(f"📍 {msisdn} @ {tower} [{enodeb_id}/{cell_id}] → {lat},{lon}")

        return insert_record(msisdn, imsi, str(enodeb_id), str(cell_id), tower, lat, lon, timestamp)

    except Exception as e:
        logger.error(f"Processing error: {e}")
        return False

def tail_and_process():
    logger.info("🚀 RADIUS Watcher starting...")
    with get_db_connection() as conn:
        create_tables_if_not_exists(conn)

    log_file = get_latest_log()
    if not log_file:
        logger.error("❌ No detail-* logs found")
        return

    logger.info(f"📄 Watching log: {log_file}")
    with open(log_file, "r") as f:
        f.seek(0, os.SEEK_END)
        buffer = []
        count = 0

        while True:
            line = f.readline()
            if not line:
                time.sleep(0.5)
                continue

            line = line.strip()
            if not line and buffer:
                entry = {}
                for raw in buffer:
                    if match := re.match(r'(\S+)\s+=\s+"?(.*?)"?$', raw):
                        entry[match[1]] = match[2]
                if process_entry(entry):
                    count += 1
                    if count % Config.LOG_EVERY == 0:
                        logger.info(f"✅ Processed {count} records")
                buffer = []
                continue

            buffer.append(line)

# ====================== MAIN ======================
if __name__ == "__main__":
    try:
        tail_and_process()
    except KeyboardInterrupt:
        logger.info("🔚 Exiting on CTRL+C")
