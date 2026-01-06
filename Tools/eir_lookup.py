#!/usr/bin/env python3
"""
EIR Lookup Module
- Device model lookup by TAC
- PostgreSQL integration
- Enhanced logging
"""

import os
import logging
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ====================== CONFIGURATION ======================
class Config:
    DB_CONFIG = {
        "dbname": "tracedb",
        "user": "enet",
        "password": os.environ.get("DB_PASSWORD", "changeme"),
        "host": "localhost",
        "port": "5432"
    }
    LOG_FILE = "logs/eir_lookup.log"
    MAX_RETRIES = 3
    RETRY_DELAY = 1

# ====================== LOGGING ======================
def setup_logging():
    """Configure logging with both file and console handlers"""
    os.makedirs("logs", exist_ok=True)
    
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    
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
            CREATE TABLE IF NOT EXISTS device_models (
                id SERIAL PRIMARY KEY,
                tac VARCHAR(8) NOT NULL UNIQUE,
                manufacturer VARCHAR(100),
                model VARCHAR(100),
                device_type VARCHAR(50),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_device_models_tac ON device_models(tac);
        """)
        conn.commit()

def lookup_device_model(tac):
    """Look up device model information by TAC"""
    if not tac or len(tac) < 8:
        logger.warning(f"Invalid TAC: {tac}")
        return "Unknown Device"
        
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT manufacturer, model, device_type
                    FROM device_models
                    WHERE tac = %s
                """, (tac[:8],))
                
                result = cur.fetchone()
                if result:
                    manufacturer, model, device_type = result
                    return f"{manufacturer} {model} ({device_type})"
                    
                logger.warning(f"No device model found for TAC: {tac}")
                return "Unknown Device"
                
    except Exception as e:
        logger.error(f"Failed to lookup device model: {e}")
        return "Unknown Device"

def update_device_model(tac, manufacturer, model, device_type):
    """Update or insert device model information"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO device_models (tac, manufacturer, model, device_type)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (tac) DO UPDATE SET
                        manufacturer = EXCLUDED.manufacturer,
                        model = EXCLUDED.model,
                        device_type = EXCLUDED.device_type,
                        updated_at = NOW()
                """, (tac[:8], manufacturer, model, device_type))
                conn.commit()
                logger.info(f"Updated device model for TAC {tac}")
                return True
    except Exception as e:
        logger.error(f"Failed to update device model: {e}")
        return False

def import_device_models(csv_file):
    """Import device models from CSV file"""
    try:
        with open(csv_file, 'r') as f:
            import csv
            reader = csv.DictReader(f)
            
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    for row in reader:
                        cur.execute("""
                            INSERT INTO device_models (tac, manufacturer, model, device_type)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (tac) DO UPDATE SET
                                manufacturer = EXCLUDED.manufacturer,
                                model = EXCLUDED.model,
                                device_type = EXCLUDED.device_type,
                                updated_at = NOW()
                        """, (
                            row['tac'][:8],
                            row.get('manufacturer', 'Unknown'),
                            row.get('model', 'Unknown'),
                            row.get('device_type', 'Unknown')
                        ))
                    conn.commit()
                    logger.info(f"Imported device models from {csv_file}")
                    return True
    except Exception as e:
        logger.error(f"Failed to import device models: {e}")
        return False

# ====================== ENTRY POINT ======================
if __name__ == "__main__":
    try:
        # Initialize database
        with get_db_connection() as conn:
            create_tables_if_not_exists(conn)
            
        # Example usage
        tac = "12345678"
        print(f"Looking up device model for TAC: {tac}")
        model = lookup_device_model(tac)
        print(f"Result: {model}")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
