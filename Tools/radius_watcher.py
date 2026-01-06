#!/usr/bin/env python3
"""
RADIUS Log Watcher Service v2
- Polls RADIUS files every few seconds
- Efficiently processes new lines
- Batch inserts to database
"""

import os
import sys
import time
import logging
from datetime import datetime
from collections import defaultdict

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Tools.radius_parser import parse_radius_record, get_db_connection

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('/home/enet/msisdn_checker/logs/radius_watcher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# RADIUS log directories
RADIUS_DIRS = [
    "/var/log/freeradius/radacct/100.64.145.34",
    "/var/log/freeradius/radacct/10.20.50.67"
]

POLL_INTERVAL = 5  # seconds
BATCH_SIZE = 500   # records per batch insert


class RadiusWatcher:
    def __init__(self):
        self.file_positions = {}
        self.record_buffer = []
        self.conn = None
        self.stats = defaultdict(int)
        self.reconnect_db()
        
    def reconnect_db(self):
        """Reconnect to database"""
        try:
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
            self.conn = get_db_connection()
            logger.info("Database connected")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            self.conn = None
    
    def get_today_file(self, dir_path):
        """Get today's detail file"""
        filename = datetime.now().strftime("detail-%Y%m%d")
        return os.path.join(dir_path, filename)
    
    def process_file(self, filepath):
        """Process new lines from file"""
        if not os.path.exists(filepath):
            return 0
            
        try:
            # Get current file size
            current_size = os.path.getsize(filepath)
            last_pos = self.file_positions.get(filepath, 0)
            
            # If file rotated (smaller), start from beginning
            if current_size < last_pos:
                last_pos = 0
                logger.info(f"File rotated: {filepath}")
            
            # Skip if no new data
            if current_size <= last_pos:
                return 0
            
            records = []
            current_record = []
            
            with open(filepath, 'r', errors='ignore') as f:
                f.seek(last_pos)
                
                for line in f:
                    # New record starts with non-whitespace (timestamp line)
                    if line and not line[0].isspace() and line.strip():
                        # Process previous record
                        if current_record:
                            parsed = parse_radius_record(current_record)
                            if parsed and parsed.get('msisdn') and parsed.get('enodeb_id'):
                                records.append(parsed)
                        current_record = []
                    else:
                        current_record.append(line)
                
                # Update position
                self.file_positions[filepath] = f.tell()
            
            return records
            
        except Exception as e:
            logger.error(f"Error processing {filepath}: {e}")
            return []
    
    def insert_records(self, records):
        """Batch insert records to database"""
        if not records:
            return 0
            
        if not self.conn:
            self.reconnect_db()
            if not self.conn:
                return 0
        
        try:
            cursor = self.conn.cursor()
            
            # Upsert to latest_traces (current location)
            latest_data = []
            for r in records:
                latest_data.append((
                    r.get('msisdn'),
                    r.get('imsi', ''),
                    str(r.get('enodeb_id', '')),
                    str(r.get('cell_id', '')),
                    r.get('timestamp', datetime.now())
                ))
            
            cursor.executemany("""
                INSERT INTO latest_traces (msisdn, imsi, enodeb_id, cell_id, timestamp, source)
                VALUES (%s, %s, %s, %s, %s, 'radius')
                ON CONFLICT (msisdn) DO UPDATE SET
                    imsi = EXCLUDED.imsi,
                    enodeb_id = EXCLUDED.enodeb_id,
                    cell_id = EXCLUDED.cell_id,
                    timestamp = EXCLUDED.timestamp,
                    source = 'radius'
                WHERE latest_traces.timestamp < EXCLUDED.timestamp
            """, latest_data)
            
            # Insert to radius_matches (history)
            history_data = []
            for r in records:
                history_data.append((
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
            
            cursor.executemany("""
                INSERT INTO radius_matches 
                (msisdn, imsi, imei, enodeb_id, cell_id, tower_name, lat, lon, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (msisdn, enodeb_id, cell_id, timestamp) DO NOTHING
            """, history_data)
            
            self.conn.commit()
            cursor.close()
            return len(records)
            
        except Exception as e:
            logger.error(f"Insert error: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            self.reconnect_db()
            return 0
    
    def run(self):
        """Main loop"""
        logger.info("RADIUS Watcher v2 starting...")
        
        # Initial catch-up from end of files
        for dir_path in RADIUS_DIRS:
            filepath = self.get_today_file(dir_path)
            if os.path.exists(filepath):
                # Start from current end (don't reprocess old data)
                self.file_positions[filepath] = os.path.getsize(filepath)
                logger.info(f"Watching: {filepath} (starting at {self.file_positions[filepath]} bytes)")
        
        last_stats_time = time.time()
        
        while True:
            try:
                all_records = []
                
                # Process each directory
                for dir_path in RADIUS_DIRS:
                    if not os.path.exists(dir_path):
                        continue
                        
                    filepath = self.get_today_file(dir_path)
                    records = self.process_file(filepath)
                    if records:
                        all_records.extend(records)
                
                # Insert if we have records
                if all_records:
                    inserted = self.insert_records(all_records)
                    self.stats['total'] += inserted
                    self.stats['imei'] += sum(1 for r in all_records if r.get('imei'))
                    
                    if inserted > 0:
                        logger.info(f"Inserted {inserted} records ({self.stats['total']} total)")
                
                # Log stats every 5 minutes
                if time.time() - last_stats_time > 300:
                    logger.info(f"Stats: {self.stats['total']} total, {self.stats['imei']} with IMEI")
                    last_stats_time = time.time()
                
                time.sleep(POLL_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                time.sleep(10)


if __name__ == "__main__":
    watcher = RadiusWatcher()
    watcher.run()
