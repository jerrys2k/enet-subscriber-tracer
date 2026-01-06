#!/usr/bin/env python3
"""
Optimized RADIUS Parser
- Efficient parsing of FreeRADIUS detail files
- Batch PostgreSQL inserts
- Proper 3GPP-User-Location-Info decoding
"""

import re
import os
from dotenv import load_dotenv
load_dotenv()
import sys
import logging
from datetime import datetime
from typing import Optional, Dict, List, Generator
import psycopg2
from psycopg2.extras import execute_values

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

# Database config
DB_CONFIG = {
    "dbname": "tracedb",
    "user": "enet", 
    "password": os.environ.get("DB_PASSWORD", "changeme"),
    "host": "localhost",
    "port": "5432"
}

def decode_eci(location_hex: str) -> Optional[Dict]:
    """
    Decode 3GPP-User-Location-Info to extract eNodeB and Cell ID
    Format: 0x82 + TAI(5 bytes) + ECGI(7 bytes)
    ECGI = MCC/MNC(3 bytes) + ECI(4 bytes)
    ECI = eNodeB(20 bits) + CellID(8 bits)
    """
    try:
        hex_clean = location_hex.replace("0x", "").replace("0X", "")
        
        if len(hex_clean) < 8:
            return None
            
        eci_hex = hex_clean[-8:]
        eci = int(eci_hex, 16)
        
        enodeb_id = (eci >> 8) & 0xFFFFF
        cell_id = eci & 0xFF
        
        return {"enodeb_id": enodeb_id, "cell_id": cell_id, "eci": eci}
    except Exception as e:
        logger.debug(f"ECI decode error: {e}")
        return None


def parse_radius_record(lines: List[str]) -> Optional[Dict]:
    """Parse a single RADIUS accounting record"""
    record = {}
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
            
        if '=' in line:
            key, _, value = line.partition('=')
            key = key.strip()
            value = value.strip().strip('"')
            
            if key == 'Calling-Station-Id':
                record['msisdn'] = value
            elif key == 'Framed-IP-Address':
                record['ip_address'] = value
            elif key == '3GPP-IMSI':
                record['imsi'] = value
            elif key == '3GPP-IMEISV' or key == '3GPP-IMEI':
                record['imei'] = value
            elif key == '3GPP-User-Location-Info':
                decoded = decode_eci(value)
                if decoded:
                    record['enodeb_id'] = decoded['enodeb_id']
                    record['cell_id'] = decoded['cell_id']
            elif key == 'Timestamp':
                try:
                    record['timestamp'] = datetime.fromtimestamp(int(value))
                except:
                    pass
            elif key == 'Acct-Status-Type':
                record['status_type'] = value
                
    if record.get('msisdn') and record.get('enodeb_id'):
        return record
    return None


def parse_radius_file(filepath: str) -> Generator[Dict, None, None]:
    """Generator that yields parsed RADIUS records from a file"""
    if not os.path.exists(filepath):
        logger.error(f"File not found: {filepath}")
        return
        
    logger.info(f"Parsing: {filepath}")
    
    current_record = []
    record_count = 0
    
    with open(filepath, 'r', errors='ignore') as f:
        for line in f:
            if line and not line[0].isspace() and line.strip():
                if current_record:
                    parsed = parse_radius_record(current_record)
                    if parsed:
                        record_count += 1
                        yield parsed
                current_record = []
            else:
                current_record.append(line)
        
        if current_record:
            parsed = parse_radius_record(current_record)
            if parsed:
                record_count += 1
                yield parsed
                
    logger.info(f"Parsed {record_count} valid records from {filepath}")


def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(**DB_CONFIG)


def upsert_latest_traces(records: List[Dict], conn=None):
    """
    Insert/update latest_traces table (matches existing schema)
    """
    if not records:
        return 0
        
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    
    try:
        cursor = conn.cursor()
        
        data = []
        for r in records:
            data.append((
                r.get('msisdn'),
                '',  # tower_name
                str(r.get('enodeb_id', '')),
                str(r.get('cell_id', '')),
                0.0,  # lat
                0.0,  # lon
                r.get('timestamp', datetime.now()),
                'radius',  # source
                r.get('imsi', ''),
                ''  # device_model
            ))
        
        query = """
            INSERT INTO latest_traces 
            (msisdn, tower_name, enodeb_id, cell_id, lat, lon, timestamp, source, imsi, device_model)
            VALUES %s
            ON CONFLICT (msisdn) DO UPDATE SET
                enodeb_id = EXCLUDED.enodeb_id,
                cell_id = EXCLUDED.cell_id,
                timestamp = EXCLUDED.timestamp,
                source = EXCLUDED.source,
                imsi = EXCLUDED.imsi,
                updated_at = NOW()
        """
        
        execute_values(cursor, query, data)
        conn.commit()
        
        count = len(data)
        cursor.close()
        
        if close_conn:
            conn.close()
            
        return count
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        if close_conn and conn:
            conn.close()
        return 0


def insert_radius_history(records: List[Dict], conn=None):
    """
    Insert into radius_matches for historical tracking (matches existing schema)
    """
    if not records:
        return 0
        
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    
    try:
        cursor = conn.cursor()
        
        data = []
        for r in records:
            data.append((
                r.get('msisdn'),
                r.get('imsi', ''),
                r.get('imei', ''),
                str(r.get('enodeb_id', '')),
                str(r.get('cell_id', '')),
                '',  # tower_name
                0.0,  # lat
                0.0,  # lon
                r.get('timestamp', datetime.now())
            ))
        
        query = """
            INSERT INTO radius_matches 
            (msisdn, imsi, imei, enodeb_id, cell_id, tower_name, lat, lon, timestamp)
            VALUES %s
            ON CONFLICT (msisdn, enodeb_id, cell_id, timestamp) DO NOTHING
        """
        
        execute_values(cursor, query, data)
        conn.commit()
        
        count = cursor.rowcount
        cursor.close()
        
        if close_conn:
            conn.close()
            
        return count
        
    except Exception as e:
        logger.error(f"History insert error: {e}")
        conn.rollback()
        if close_conn and conn:
            conn.close()
        return 0


def process_file(filepath: str, batch_size: int = 1000):
    """Process a RADIUS file with batch inserts"""
    conn = get_db_connection()
    
    batch = []
    total_processed = 0
    total_inserted = 0
    
    for record in parse_radius_file(filepath):
        batch.append(record)
        
        if len(batch) >= batch_size:
            inserted = upsert_latest_traces(batch, conn)
            insert_radius_history(batch, conn)
            
            total_processed += len(batch)
            total_inserted += inserted
            
            if total_processed % 10000 == 0:
                logger.info(f"Progress: {total_processed:,} records processed")
            batch = []
    
    if batch:
        inserted = upsert_latest_traces(batch, conn)
        insert_radius_history(batch, conn)
        total_processed += len(batch)
        total_inserted += inserted
    
    conn.close()
    logger.info(f"Complete: {total_processed:,} total, {total_inserted:,} inserted/updated")
    return total_processed, total_inserted


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python radius_parser.py <radius_detail_file>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    process_file(filepath)
