#!/usr/bin/env python3
"""
RADIUS Log Debug Parser
- Debug tool for parsing RADIUS logs
- PostgreSQL integration
- Enhanced logging
"""

import sys
import re
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Tuple

import psycopg2
from dotenv import load_dotenv

# Enable Tools/ path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Tools.cell_mapper import decode_location_info
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
    LOG_FILE = "logs/radius_debug.log"
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

def insert_record(msisdn: str, imsi: str, enodeb_id: str, cell_id: str, 
                 tower_name: str, lat: float, lon: float, timestamp: str,
                 device_model: str = "Unknown") -> bool:
    """Insert a single record into both tables"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Insert into radius_matches
                cur.execute("""
                    INSERT INTO radius_matches 
                    (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT unique_radius_entry DO NOTHING
                """, (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp))
                
                # Upsert into latest_traces
                cur.execute("""
                    INSERT INTO latest_traces 
                    (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp, source, device_model)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'debug', %s)
                    ON CONFLICT (msisdn) DO UPDATE SET
                        imsi = EXCLUDED.imsi,
                        enodeb_id = EXCLUDED.enodeb_id,
                        cell_id = EXCLUDED.cell_id,
                        tower_name = EXCLUDED.tower_name,
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        timestamp = EXCLUDED.timestamp,
                        source = 'debug',
                        device_model = EXCLUDED.device_model,
                        updated_at = NOW()
                """, (msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp, device_model))
                
                conn.commit()
                return True
    except Exception as e:
        logger.error(f"Failed to insert record: {e}")
        return False

def decode_enodeb_cellid(uli_hex):
    if uli_hex.startswith('0x'):
        uli_hex = uli_hex[2:]
    eci_hex = uli_hex[-8:]
    eci = int(eci_hex, 16)
    enodeb_id = eci >> 8
    cell_id = eci & 0xFF
    return enodeb_id, cell_id

# ====================== FILE PROCESSING ======================
def process_block(content: str) -> bool:
    """Process a single RADIUS block"""
    try:
        # Extract fields
        msisdn_match = re.search(r'Calling-Station-Id\s+=\s+"(\d{10})"', content)
        location_match = re.search(r'3GPP-User-Location-Info\s+=\s+0x([0-9A-Fa-f]+)', content)
        timestamp_match = re.search(r'Timestamp\s+=\s+(\d+)', content)
        imei_match = re.search(r'3GPP-IMEISV\s*=\s*"(\d+)"', content)
        
        if not all([msisdn_match, location_match, timestamp_match]):
            return False
            
        msisdn = msisdn_match.group(1)
        enodeb_id, cell_id = None, None
        if location_match:
            try:
                uli_hex = location_match.group(0).split('=')[1].strip()
                enodeb_id, cell_id = decode_enodeb_cellid(uli_hex)
            except Exception as e:
                logger.warning(f"Failed to decode ULI: {uli_hex} ({e})")
        
        timestamp = datetime.fromtimestamp(int(timestamp_match.group(1))).strftime("%b %d %Y %H:%M:%S GMT-04")
        
        # Get device model
        device_model = "Unknown Device"
        if imei_match:
            imei = imei_match.group(1)
            tac = imei[:8]
            device_model = lookup_device_model(tac)
            
        # Decode location
        decoded = decode_location_info(location_match.group(1))
        if not decoded or len(decoded) != 5:
            logger.warning(f"Invalid location data for {msisdn}: {location_match.group(1)}")
            return False
            
        tower, lat, lon = decoded[:3]
        
        # Insert record
        if insert_record(msisdn, "", str(enodeb_id), str(cell_id), tower, lat, lon, timestamp, device_model):
            logger.info(f"Processed {msisdn} → {tower} (eNB {enodeb_id}, Cell {cell_id}) at ({lat}, {lon}) [{timestamp}]")
            logger.info(f"Device: {device_model} (IMEI {imei_match.group(1) if imei_match else 'Unknown'})")
            return True
            
        return False
        
    except Exception as e:
        logger.error(f"Failed to process block: {e}")
        return False

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        logger.error("Usage: python3 parse_radius_logs_debug.py <detail-file>")
        sys.exit(1)
        
    detail_file = sys.argv[1]
    logger.info(f"Processing {detail_file}")
    
    try:
        # Initialize database
        with get_db_connection() as conn:
            create_tables_if_not_exists(conn)
            
        # Process file
        with open(detail_file, 'r', encoding='utf-8', errors='ignore') as f:
            block = []
            processed = 0
            
            for line in f:
                if line.strip() == "":
                    if block:
                        if process_block("\n".join(block)):
                            processed += 1
                        block = []
                else:
                    block.append(line)
                    
            # Process final block
            if block:
                if process_block("\n".join(block)):
                    processed += 1
                    
        logger.info(f"Completed processing {detail_file}: {processed} records processed")
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

# ====================== ENTRY POINT ======================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
