#!/usr/bin/env python3
"""
Enhanced RADIUS Backfill Processor
- Parallel log processing with resume capability
- Bulk database operations with validation
- Comprehensive monitoring and error handling
"""

import os
import re
import sys
import time
import signal
import psutil
import logging
import threading
from datetime import datetime, timezone, timedelta
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
from dotenv import load_dotenv
from pydantic import BaseModel, field_validator, ValidationError

# Ensure Tools/ is importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from Tools.cell_mapper import decode_location_info
from Tools.tower_index_loader import load_tower_index
from Tools.eir_lookup import lookup_device_model

# ====================== CONFIG ======================
class Config:
    DB_CONFIG = {
        "dbname": "tracedb",
        "user": "enet",
        "password": os.environ.get("DB_PASSWORD", "changeme"),
        "host": "localhost",
        "port": "5432"
    }
    LOG_FILE = "logs/backfill.log"
    MAX_RETRIES = 3
    RETRY_DELAY = 1
    BATCH_SIZE = 1000
    ENABLE_PURGE = True
    PURGE_AGE_DAYS = 30
    MAX_WORKERS = min(cpu_count(), 8)
    LOG_DIR = "/var/log/freeradius/radacct"
    DETAIL_PREFIX = "detail-"
    LOCK_FILE = "/tmp/msisdn_backfill.lock"
    PROGRESS_FILE = "logs/backfill_progress.txt"

# ====================== MODELS ======================
class RadiusRecord(BaseModel):
    msisdn: str
    imsi: str
    enodeb_id: str
    cell_id: str
    tower_name: str
    lat: float
    lon: float
    timestamp: str
    device_model: str

    @field_validator('msisdn')
    @classmethod
    def validate_msisdn(cls, v):
        if not v or len(v) < 10:
            raise ValueError("Invalid MSISDN length")
        return v

    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v):
        try:
            datetime.strptime(v, "%b %d %Y %H:%M:%S GMT-04")
            return v
        except ValueError:
            raise ValueError("Invalid timestamp format")

# ====================== LOGGING ======================
class LogFormatter(logging.Formatter):
    """Custom formatter that adds colors to log levels"""
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
    """Configure logging with both file and console handlers"""
    os.makedirs("logs", exist_ok=True)
    
    formatter = LogFormatter("%(asctime)s | %(levelname)s | %(message)s")
    
    # File handler
    file_handler = logging.FileHandler(Config.LOG_FILE)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger(__name__)

log = setup_logging()

# ====================== DATABASE OPERATIONS ======================
def get_db_connection():
    """Get connection with retry logic"""
    for attempt in range(Config.MAX_RETRIES):
        try:
            conn = psycopg2.connect(**Config.DB_CONFIG)
            conn.autocommit = False
            return conn
        except Exception as e:
            if attempt == Config.MAX_RETRIES - 1:
                log.error(f"Failed to establish DB connection after {Config.MAX_RETRIES} attempts")
                raise
            log.warning(f"DB connection failed (attempt {attempt + 1}): {e}")
            time.sleep(Config.RETRY_DELAY)
    raise ConnectionError("Failed to establish DB connection")

def create_tables_if_not_exists(conn):
    """Ensure required tables exist with proper constraints"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS radius_matches (
                id SERIAL PRIMARY KEY,
                msisdn VARCHAR(15) NOT NULL,
                imsi VARCHAR(15),
                enodeb_id VARCHAR(20) NOT NULL,
                cell_id VARCHAR(10) NOT NULL,
                tower_name TEXT,
                lat FLOAT,
                lon FLOAT,
                timestamp TIMESTAMPTZ NOT NULL,
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
            
            CREATE TABLE IF NOT EXISTS backfill_audit (
                filename TEXT PRIMARY KEY,
                processed INT NOT NULL,
                inserted INT NOT NULL,
                deduplicated INT NOT NULL,
                runtime_seconds INT NOT NULL,
                backfill_time TIMESTAMPTZ DEFAULT now()
            );
            
            CREATE INDEX IF NOT EXISTS idx_radius_msisdn ON radius_matches(msisdn);
            CREATE INDEX IF NOT EXISTS idx_radius_timestamp ON radius_matches(timestamp);
        """)
        conn.commit()

def bulk_insert_records(records: List[RadiusRecord]) -> int:
    """Efficient bulk insert with conflict handling for both radius_matches and latest_traces."""
    if not records:
        return 0
        
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Prepare data
                matches_data = [
                    (r.msisdn, r.imsi, r.enodeb_id, r.cell_id,
                     r.tower_name, r.lat, r.lon, r.timestamp)
                    for r in records
                ]

                traces_data = [
                    (r.msisdn, r.imsi, r.enodeb_id, r.cell_id,
                     r.tower_name, r.lat, r.lon, r.timestamp, r.device_model)
                    for r in records
                ]

                # Bulk insert into radius_matches (skip duplicates)
                cur.executemany("""
                    INSERT INTO radius_matches 
                    (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT unique_radius_entry DO NOTHING
                """, matches_data)

                # Bulk upsert into latest_traces
                cur.executemany("""
                    INSERT INTO latest_traces 
                    (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp, source, device_model)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'backfill', %s)
                    ON CONFLICT (msisdn) DO UPDATE SET
                        imsi = EXCLUDED.imsi,
                        enodeb_id = EXCLUDED.enodeb_id,
                        cell_id = EXCLUDED.cell_id,
                        tower_name = EXCLUDED.tower_name,
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        timestamp = EXCLUDED.timestamp,
                        source = 'backfill',
                        device_model = EXCLUDED.device_model,
                        updated_at = NOW()
                """, traces_data)

                inserted = cur.rowcount
                conn.commit()
                return inserted
    except Exception as e:
        log.error(f"Bulk insert failed: {e}")
        raise

# ====================== FILE PROCESSING ======================
def get_all_log_files() -> Dict[str, List[str]]:
    """Scan log directory for today's files organized by PGW IP"""
    today_str = datetime.now(timezone(timedelta(hours=-4))).strftime("%Y%m%d")
    log.info(f"Scanning for RADIUS files in {Config.LOG_DIR}")
    
    pgw_files = {}
    try:
        for ip_dir in os.listdir(Config.LOG_DIR):
            full_path = Path(Config.LOG_DIR) / ip_dir
            if not full_path.is_dir():
                continue
                
            matched_files = [
                str(f) for f in full_path.glob(f"{Config.DETAIL_PREFIX}{today_str}*") 
                if f.is_file()
            ]
            
            if matched_files:
                log.info(f"Found {len(matched_files)} files in {ip_dir}")
                pgw_files[ip_dir] = sorted(matched_files)  # Sort files for consistent processing
                
    except Exception as e:
        log.error(f"Directory scan failed: {e}")
        raise
        
    return pgw_files

def parse_entry(lines: List[str]) -> dict:
    """Parse multi-line RADIUS entry into dict"""
    entry = {}
    for line in lines:
        if match := re.match(r'(\S+)\s+=\s+"?(.*?)"?$', line):
            key, value = match.groups()
            entry[key] = value
    return entry

def read_log_entries(file_path: str) -> Generator[List[str], None, None]:
    """Generator that yields complete RADIUS entries from file"""
    buffer = []
    try:
        with open(file_path) as f:
            for line in f:
                line = line.strip()
                if not line and buffer:
                    yield buffer
                    buffer = []
                elif line:
                    buffer.append(line)
                    
        if buffer:  # Final entry if file doesn't end with newline
            yield buffer
    except Exception as e:
        log.error(f"Failed reading {file_path}: {e}")
        raise

def process_timestamp(raw_ts: str) -> Optional[str]:
    """Convert RADIUS timestamp to local timezone format"""
    try:
        ts_obj = datetime.strptime(raw_ts, "%b %d %Y %H:%M:%S UTC")
        local_dt = ts_obj.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-4)))
        
        if local_dt.date() != datetime.now(timezone(timedelta(hours=-4))).date():
            return None
            
        return local_dt.strftime("%b %d %Y %H:%M:%S GMT-04")
    except Exception as e:
        log.warning(f"Invalid timestamp format {raw_ts}: {e}")
        return None

def process_location(raw_loc: str) -> Optional[Tuple]:
    """Decode 3GPP location information"""
    try:
        decoded = decode_location_info(raw_loc)
        if not decoded or len(decoded) != 5:
            log.warning(f"Invalid location data: {raw_loc}")
            return None
        return decoded
    except Exception as e:
        log.warning(f"Location decoding failed: {e}")
        return None

def decode_enodeb_cellid(uli_hex):
    if uli_hex.startswith('0x'):
        uli_hex = uli_hex[2:]
    eci_hex = uli_hex[-8:]
    eci = int(eci_hex, 16)
    enodeb_id = eci >> 8
    cell_id = eci & 0xFF
    return enodeb_id, cell_id

def build_record(entry: dict) -> Optional[RadiusRecord]:
    """Validate and transform raw entry into structured record"""
    try:
        msisdn = entry.get("Calling-Station-Id", "").strip()
        imsi = (entry.get("3GPP-IMSI") or entry.get("User-Name", "")).strip()
        raw_loc = entry.get("3GPP-User-Location-Info", "").strip()
        raw_ts = entry.get("Event-Timestamp", "").strip()
        imei_raw = entry.get("3GPP-IMEISV", "").strip()
        
        if not all([msisdn, raw_loc, raw_ts]):
            return None
            
        timestamp = process_timestamp(raw_ts)
        if not timestamp:
            return None
            
        location_data = process_location(raw_loc)
        if not location_data:
            return None
            
        tower, enodeb, cell_id, lat, lon = location_data
        enodeb = str(enodeb)
        cell_id = str(cell_id)
        model = lookup_device_model(imei_raw[:8]) if imei_raw else "Unknown"
        
        return RadiusRecord(
            msisdn=msisdn,
            imsi=imsi,
            enodeb_id=enodeb,
            cell_id=cell_id,
            tower_name=tower,
            lat=lat,
            lon=lon,
            timestamp=timestamp,
            device_model=model
        )
    except ValidationError as e:
        log.warning(f"Invalid record: {e}")
        return None

# ====================== PROGRESS TRACKING ======================
class ProgressTracker:
    def __init__(self):
        self.file = Path(Config.PROGRESS_FILE)
        self.lock = threading.Lock()
        
    def save(self, filename: str, position: int):
        with self.lock:
            try:
                with self.file.open("a") as f:
                    f.write(f"{filename}|{position}\n")
            except Exception as e:
                log.error(f"Failed to save progress: {e}")

    def load(self) -> Dict[str, int]:
        if not self.file.exists():
            return {}
            
        try:
            with self.file.open() as f:
                return {
                    parts[0]: int(parts[1])
                    for line in f
                    if (parts := line.strip().split("|")) and len(parts) == 2
                }
        except Exception as e:
            log.error(f"Failed to load progress: {e}")
            return {}

    def clear(self):
        try:
            if self.file.exists():
                self.file.unlink()
        except Exception as e:
            log.error(f"Failed to clear progress: {e}")

# ====================== CORE PROCESSING ======================
def process_log_file(log_file: str) -> Tuple[int, int]:
    """Process a single log file with resume support"""
    tracker = ProgressTracker()
    resume_pos = tracker.load().get(log_file, 0)
    batch = []
    processed = inserted = 0
    start_time = time.time()
    file_name = Path(log_file).name

    try:
        log.info(f"Processing {file_name} (resume position: {resume_pos})")
        
        for entry_num, entry_lines in enumerate(read_log_entries(log_file)):
            entry = parse_entry(entry_lines)
            record = build_record(entry)
            
            if record:
                batch.append(record)
                processed += 1
                
                if len(batch) >= Config.BATCH_SIZE:
                    inserted += bulk_insert_records(batch)
                    batch.clear()
                    tracker.save(log_file, entry_num + 1)
                    
                    if processed % 5000 == 0:
                        log.info(f"Processed {processed} entries from {file_name}")

        # Final batch
        if batch:
            inserted += bulk_insert_records(batch)
            
        # Record completion
        runtime = int(time.time() - start_time)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO backfill_audit
                    (filename, processed, inserted, deduplicated, runtime_seconds)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (filename) DO NOTHING
                """, (log_file, processed, inserted, processed - inserted, runtime))
                conn.commit()
                
        log.info(f"Completed {file_name}: {processed} processed, {inserted} inserted")
        
        # Cleanup
        try:
            Path(log_file).unlink()
            log.info(f"Removed processed file: {file_name}")
        except Exception as e:
            log.error(f"File deletion failed: {e}")

    except Exception as e:
        log.error(f"Failed processing {file_name}: {e}")
        raise
        
    return (processed, inserted)

# ====================== SYSTEM MONITORING ======================
def log_system_metrics():
    """Log CPU, memory, and database health"""
    try:
        cpu = psutil.cpu_percent()
        mem = psutil.virtual_memory().percent
        disk = psutil.disk_usage('/').percent
        log.info(f"System Load: CPU={cpu}% MEM={mem}% DISK={disk}%")
        
        # Database health check
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) FROM radius_matches")
                    count = cur.fetchone()[0]
                    log.info(f"Database Stats: {count} radius records")
        except Exception as e:
            log.error(f"DB health check failed: {e}")
            
    except Exception as e:
        log.error(f"System metrics failed: {e}")

def monitor_heartbeat():
    """Background thread for periodic monitoring"""
    while Path(Config.LOCK_FILE).exists():
        log_system_metrics()
        time.sleep(60)

# ====================== MAINTENANCE ======================
def perform_maintenance():
    """Database cleanup and optimization"""
    if not Config.ENABLE_PURGE:
        return
        
    try:
        with get_db_connection() as conn:
            # Purge old audit logs inside a transaction
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM backfill_audit 
                    WHERE backfill_time < NOW() - INTERVAL '%s days'
                """, [Config.PURGE_AGE_DAYS])
                log.info(f"Purged {cur.rowcount} old audit records")

            # VACUUM must run outside transaction block
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("VACUUM ANALYZE radius_matches")
                log.info("Optimized radius_matches table")
                
    except Exception as e:
        log.error(f"Maintenance failed: {e}")

# ====================== MAIN EXECUTION ======================
def backfill():
    """Main backfill orchestration"""
    # Check for existing lock
    if Path(Config.LOCK_FILE).exists():
        log.warning("Backfill already running (lock file exists)")
        return
        
    # Create lock file
    try:
        with open(Config.LOCK_FILE, "w") as f:
            f.write(str(os.getpid()))
    except Exception as e:
        log.error(f"Failed to create lock file: {e}")
        return
        
    try:
        log.info(f"Starting backfill (PID: {os.getpid()})")
        
        # Initialize
        with get_db_connection() as conn:
            create_tables_if_not_exists(conn)
            
        # Start monitoring
        threading.Thread(target=monitor_heartbeat, daemon=True).start()
        
        # Process files
        all_logs = get_all_log_files()
        if not all_logs:
            log.warning("No log files found to process")
            return
            
        total_processed = total_inserted = 0
        
        for pgw_ip, files in all_logs.items():
            log.info(f"Processing {len(files)} files from {pgw_ip}")
            
            with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
                future_to_file = {executor.submit(process_log_file, f): f for f in files}
                
                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        processed, inserted = future.result()
                        total_processed += processed
                        total_inserted += inserted
                        log.info(f"Completed {Path(file_path).name}: {processed}/{inserted}")
                    except Exception as e:
                        log.error(f"Failed processing {file_path}: {e}")
        
        # Final report
        log.info(f"Backfill complete. Total: {total_processed} processed, {total_inserted} inserted")
        
        # Maintenance
        perform_maintenance()
        
    except Exception as e:
        log.error(f"Backfill failed: {e}")
    finally:
        # Cleanup
        try:
            Path(Config.LOCK_FILE).unlink()
            ProgressTracker().clear()
        except Exception as e:
            log.error(f"Cleanup failed: {e}")
            
        log.info("Backfill shutdown complete")

# ====================== ENTRY POINT ======================
if __name__ == "__main__":
    # Set timezone
    os.environ["TZ"] = "Etc/GMT+4"
    time.tzset()
    
    # Execute
    backfill()
