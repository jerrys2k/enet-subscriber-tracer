#!/usr/bin/env python3
"""
RADIUS Log Watcher Service
- Tails RADIUS detail files in real-time
- Parses and inserts to database
- Runs as systemd service
"""

import os
import sys
import time
import logging
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Tools.radius_parser import parse_radius_record, upsert_latest_traces, insert_radius_history, get_db_connection

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


class RadiusFileHandler(FileSystemEventHandler):
    """Handle changes to RADIUS detail files"""
    
    def __init__(self):
        self.file_positions = {}
        self.conn = None
        self.buffer = {}
        self.reconnect_db()
        
    def reconnect_db(self):
        """Reconnect to database"""
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        try:
            self.conn = get_db_connection()
            logger.info("Database connected")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            self.conn = None
    
    def on_modified(self, event):
        """Called when a file is modified"""
        if event.is_directory:
            return
            
        filepath = event.src_path
        if not filepath.endswith(datetime.now().strftime("detail-%Y%m%d")):
            return
            
        self.process_new_lines(filepath)
    
    def process_new_lines(self, filepath):
        """Read new lines from file"""
        try:
            # Get last position
            last_pos = self.file_positions.get(filepath, 0)
            
            with open(filepath, 'r', errors='ignore') as f:
                # If file is new or truncated, start from beginning
                f.seek(0, 2)  # Go to end
                current_size = f.tell()
                
                if current_size < last_pos:
                    last_pos = 0
                    
                f.seek(last_pos)
                
                # Initialize buffer for this file
                if filepath not in self.buffer:
                    self.buffer[filepath] = []
                
                records_to_insert = []
                
                for line in f:
                    # New record starts with date (no leading whitespace)
                    if line and not line[0].isspace() and line.strip():
                        # Process previous record
                        if self.buffer[filepath]:
                            parsed = parse_radius_record(self.buffer[filepath])
                            if parsed:
                                records_to_insert.append(parsed)
                        self.buffer[filepath] = []
                    else:
                        self.buffer[filepath].append(line)
                
                # Update position
                self.file_positions[filepath] = f.tell()
                
                # Insert records
                if records_to_insert:
                    if not self.conn:
                        self.reconnect_db()
                    
                    if self.conn:
                        try:
                            upsert_latest_traces(records_to_insert, self.conn)
                            insert_radius_history(records_to_insert, self.conn)
                            logger.debug(f"Inserted {len(records_to_insert)} records")
                        except Exception as e:
                            logger.error(f"Insert error: {e}")
                            self.reconnect_db()
                            
        except Exception as e:
            logger.error(f"Error processing {filepath}: {e}")


def main():
    logger.info("RADIUS Watcher starting...")
    
    handler = RadiusFileHandler()
    observer = Observer()
    
    for dir_path in RADIUS_DIRS:
        if os.path.exists(dir_path):
            observer.schedule(handler, dir_path, recursive=False)
            logger.info(f"Watching: {dir_path}")
        else:
            logger.warning(f"Directory not found: {dir_path}")
    
    observer.start()
    logger.info("RADIUS Watcher running. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        observer.stop()
    
    observer.join()
    logger.info("RADIUS Watcher stopped")


if __name__ == "__main__":
    main()
