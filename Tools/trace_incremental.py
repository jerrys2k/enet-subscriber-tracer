# ====================== IMPORTS ======================
import os
import re
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
from dotenv import load_dotenv
from Tools.cell_mapper import decode_location_info
from Tools.tower_index_loader import load_tower_index

# ====================== ENV + CONFIG ======================
load_dotenv()

class Config:
    DB_CONFIG = {
        "dbname": "tracedb",
        "user": "enet",
        "password": os.environ.get("DB_PASSWORD", "changeme"),
        "host": "localhost",
        "port": "5432"
    }
    RADIUS_DIR = "/var/log/freeradius/radacct"
    STATE_FILE = "data/.last_parsed_timestamp"
    LOG_FILE = "logs/incremental_pg.log"
    LOG_EVERY = 1000
    MAX_WORKERS = min(os.cpu_count(), 8)
    BATCH_SIZE = 1000
    MAX_RETRIES = 3
    RETRY_DELAY = 1

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
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger(__name__)

logger = setup_logging()

# ====================== DATABASE OPERATIONS ======================
def get_db_connection():
    """Get a connection to the PostgreSQL database with retry logic"""
    for attempt in range(Config.MAX_RETRIES):
        try:
            conn = psycopg2.connect(**Config.DB_CONFIG)
            return conn
        except Exception as e:
            if attempt == Config.MAX_RETRIES - 1:
                logger.error(f"Failed to connect to database after {Config.MAX_RETRIES} attempts: {e}")
                raise
            logger.warning(f"Database connection failed (attempt {attempt + 1}): {e}")
            time.sleep(Config.RETRY_DELAY)
    
    raise ConnectionError("Failed to establish database connection")

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
            
            CREATE INDEX IF NOT EXISTS idx_radius_msisdn ON radius_matches(msisdn);
            CREATE INDEX IF NOT EXISTS idx_radius_timestamp ON radius_matches(timestamp);
        """)
        conn.commit()

def bulk_insert_records(records: List[Tuple]) -> int:
    """Efficient bulk insert with conflict handling"""
    if not records:
        return 0
        
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Bulk insert into radius_matches
                cur.executemany("""
                    INSERT INTO radius_matches 
                    (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT unique_radius_entry DO NOTHING
                """, records)
                
                # Bulk upsert into latest_traces
                cur.executemany("""
                    INSERT INTO latest_traces 
                    (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'incremental')
                    ON CONFLICT (msisdn) DO UPDATE SET
                        imsi = EXCLUDED.imsi,
                        enodeb_id = EXCLUDED.enodeb_id,
                        cell_id = EXCLUDED.cell_id,
                        tower_name = EXCLUDED.tower_name,
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        timestamp = EXCLUDED.timestamp,
                        source = 'incremental',
                        updated_at = NOW()
                """, records)
                
                inserted = cur.rowcount
                conn.commit()
                return inserted
    except Exception as e:
        logger.error(f"Bulk insert failed: {e}")
        raise

# ====================== STATE MANAGEMENT ======================
def read_last_timestamp() -> datetime:
    """Read the last processed timestamp from state file"""
    if not os.path.exists(Config.STATE_FILE):
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    try:
        with open(Config.STATE_FILE, "r") as f:
            ts = int(f.read().strip())
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception as e:
        logger.error(f"Failed to read last timestamp: {e}")
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

def write_last_timestamp(timestamp: datetime):
    """Write the last processed timestamp to state file"""
    try:
        os.makedirs(os.path.dirname(Config.STATE_FILE), exist_ok=True)
        with open(Config.STATE_FILE, "w") as f:
            f.write(str(int(timestamp.timestamp())))
        logger.debug(f"Updated last timestamp to {timestamp}")
    except Exception as e:
        logger.error(f"Failed to write last timestamp: {e}")

# ====================== FILE PROCESSING ======================
def get_log_files() -> List[str]:
    """Get list of log files to process"""
    log_files = []
    try:
        for root, _, files in os.walk(Config.RADIUS_DIR):
            for file in files:
                if file.endswith(".log"):
                    log_files.append(os.path.join(root, file))
        return sorted(log_files)
    except Exception as e:
        logger.error(f"Failed to get log files: {e}")
        raise

def parse_block(block: str) -> Optional[Dict]:
    """Parse a block of RADIUS log entries"""
    try:
        entry = {}
        for line in block.strip().split("\n"):
            if match := re.match(r'(\S+)\s+=\s+"?(.*?)"?$', line):
                key, val = match.groups()
                entry[key] = val
        return entry
    except Exception as e:
        logger.warning(f"Failed to parse block: {e}")
        return None

def process_timestamp(raw_ts: str) -> Optional[datetime]:
    """Convert RADIUS timestamp to datetime object"""
    try:
        if "UTC" in raw_ts:
            dt = datetime.strptime(raw_ts, "%b %d %Y %H:%M:%S %Z")
        else:
            dt = datetime.utcfromtimestamp(int(raw_ts))
        return dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        logger.warning(f"Invalid timestamp format {raw_ts}: {e}")
        return None

def process_location(raw_loc: str) -> Optional[Tuple]:
    """Decode 3GPP location information"""
    try:
        decoded = decode_location_info(raw_loc)
        if not decoded or len(decoded) != 5:
            logger.warning(f"Invalid location data: {raw_loc}")
            return None
        return decoded
    except Exception as e:
        logger.warning(f"Location decoding failed: {e}")
        return None

def decode_enodeb_cellid(uli_hex):
    if uli_hex.startswith('0x'):
        uli_hex = uli_hex[2:]
    eci_hex = uli_hex[-8:]
    eci = int(eci_hex, 16)
    enodeb_id = eci >> 8
    cell_id = eci & 0xFF
    return enodeb_id, cell_id

def process_log_file(log_file: str, since_ts: datetime) -> Tuple[int, int]:
    """Process a single log file"""
    logger.info(f"Processing {log_file}")
    processed = inserted = 0
    batch = []
    
    try:
        with open(log_file, 'r') as f:
            blocks = f.read().split("\n\n")
            
        for block in blocks:
            entry = parse_block(block)
            if not entry:
                continue
                
            # Extract required fields
            msisdn = entry.get("Calling-Station-Id", "").strip()
            imsi = (entry.get("3GPP-IMSI") or entry.get("User-Name", "")).strip()
            raw_loc = entry.get("3GPP-User-Location-Info", "").strip()
            raw_ts = entry.get("Event-Timestamp", "").strip()
            
            if not all([msisdn, raw_loc, raw_ts]):
                continue
                
            # Process timestamp
            timestamp = process_timestamp(raw_ts)
            if not timestamp or timestamp <= since_ts:
                continue
                
            # Process location
            location_data = process_location(raw_loc)
            if not location_data:
                continue
                
            tower, enodeb, cell_id, lat, lon = location_data
            timestamp_str = timestamp.astimezone(timezone(timedelta(hours=-4))).strftime("%b %d %Y %H:%M:%S GMT-04")
            
            # Decode enodeb_id and cell_id
            enodeb_id, cell_id = decode_enodeb_cellid(raw_loc)
            
            # Add to batch
            batch.append((msisdn, imsi, str(enodeb_id), str(cell_id), tower, lat, lon, timestamp_str))
            processed += 1
            
            # Process batch if full
            if len(batch) >= Config.BATCH_SIZE:
                inserted += bulk_insert_records(batch)
                batch.clear()
                
                if processed % Config.LOG_EVERY == 0:
                    logger.info(f"Processed {processed} entries from {log_file}")
        
        # Process final batch
        if batch:
            inserted += bulk_insert_records(batch)
            
        logger.info(f"Completed {log_file}: {processed} processed, {inserted} inserted")
        return processed, inserted
        
    except Exception as e:
        logger.error(f"Failed to process {log_file}: {e}")
        raise

# ====================== MAIN PROCESSING ======================
def process_radius_logs():
    """Process new radius logs and update the database"""
    logger.info("Starting radius log processing")
    
    try:
        # Initialize
        with get_db_connection() as conn:
            create_tables_if_not_exists(conn)
        
        # Get last processed timestamp
        last_ts = read_last_timestamp()
        logger.info(f"Last processed timestamp: {last_ts.isoformat()}")
        
        # Get log files
        log_files = get_log_files()
        if not log_files:
            logger.info("No new log files to process")
            return
            
        logger.info(f"Found {len(log_files)} log files to process")
        
        # Process files in parallel
        total_processed = total_inserted = 0
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_file = {executor.submit(process_log_file, f, last_ts): f for f in log_files}
            
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    processed, inserted = future.result()
                    total_processed += processed
                    total_inserted += inserted
                except Exception as e:
                    logger.error(f"Failed processing {file_path}: {e}")
        
        # Update timestamp
        write_last_timestamp(datetime.now(timezone.utc))
        
        logger.info(f"Processing complete. Total: {total_processed} processed, {total_inserted} inserted")
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

# ====================== ENTRY POINT ======================
if __name__ == "__main__":
    try:
        process_radius_logs()
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
