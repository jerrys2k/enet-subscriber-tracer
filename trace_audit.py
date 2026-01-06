#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import psycopg2
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/trace_audit.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "dbname": "tracedb",
    "user": "enet",
    "password": "${DB_PASSWORD}",
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

def audit_traces():
    """Audit trace records and clean up old data"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get count of records older than 30 days
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(
            """
            SELECT COUNT(*) 
            FROM latest_traces 
            WHERE timestamp < %s
            """,
            (thirty_days_ago,)
        )
        old_records = cursor.fetchone()[0]
        
        if old_records > 0:
            logger.info(f"Found {old_records} records older than 30 days")
            
            # Delete old records
            cursor.execute(
                """
                DELETE FROM latest_traces 
                WHERE timestamp < %s
                """,
                (thirty_days_ago,)
            )
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"Deleted {deleted_count} old records")
        
        # Get total record count
        cursor.execute("SELECT COUNT(*) FROM latest_traces")
        total_records = cursor.fetchone()[0]
        logger.info(f"Total records in database: {total_records}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Audit failed: {e}")
        raise

if __name__ == "__main__":
    try:
        logger.info("Starting trace audit")
        audit_traces()
        logger.info("Trace audit completed successfully")
    except Exception as e:
        logger.error(f"Trace audit failed: {e}")
        exit(1)
