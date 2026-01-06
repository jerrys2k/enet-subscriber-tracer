#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import psycopg2
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/trace_audit_fast.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "dbname": "tracedb",
    "user": "enet",
    "password": os.environ.get("DB_PASSWORD", "changeme"),
    "host": "localhost",
    "port": "5432"
}

def get_db_connection():
    """Create a database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def process_chunk(chunk):
    """Process a chunk of records"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get count of records older than 30 days in this chunk
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(
            """
            SELECT COUNT(*) 
            FROM latest_traces 
            WHERE timestamp < %s AND id >= %s AND id < %s
            """,
            (thirty_days_ago, chunk[0], chunk[1])
        )
        old_records = cursor.fetchone()[0]
        
        if old_records > 0:
            logger.info(f"Found {old_records} old records in chunk {chunk[0]}-{chunk[1]}")
            
            # Delete old records in this chunk
            cursor.execute(
                """
                DELETE FROM latest_traces 
                WHERE timestamp < %s AND id >= %s AND id < %s
                """,
                (thirty_days_ago, chunk[0], chunk[1])
            )
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"Deleted {deleted_count} old records from chunk {chunk[0]}-{chunk[1]}")
        
        cursor.close()
        conn.close()
        return old_records
        
    except Exception as e:
        logger.error(f"Failed to process chunk {chunk[0]}-{chunk[1]}: {e}")
        return 0

def audit_traces_fast():
    """Audit trace records using parallel processing"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get total number of records
        cursor.execute("SELECT COUNT(*) FROM latest_traces")
        total_records = cursor.fetchone()[0]
        logger.info(f"Total records in database: {total_records}")
        
        # Calculate chunks
        chunk_size = 10000
        chunks = [(i, i + chunk_size) for i in range(0, total_records, chunk_size)]
        
        # Process chunks in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(process_chunk, chunks))
        
        total_deleted = sum(results)
        logger.info(f"Total records deleted: {total_deleted}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Fast audit failed: {e}")
        raise

if __name__ == "__main__":
    try:
        logger.info("Starting fast trace audit")
        audit_traces_fast()
        logger.info("Fast trace audit completed successfully")
    except Exception as e:
        logger.error(f"Fast trace audit failed: {e}")
        exit(1)
