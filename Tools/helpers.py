import os
import sqlite3
import psycopg2
import pandas as pd
import sqlite3
from datetime import datetime
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

# ====================== CACHE VARIABLES ======================
_PREFIX_CACHE = None
_LAST_REFRESH = None

# ====================== DATABASE CONFIG ======================
DB_CONFIG = {
    "dbname": "tracedb",
    "user": "enet",
    "password": "${DB_PASSWORD}",
    "host": "localhost",
    "port": "5432"
}

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

# ====================== PREFIX MAP LOADER ======================
def load_prefix_map(refresh_cache=False):
    global _PREFIX_CACHE, _LAST_REFRESH

    if _PREFIX_CACHE is not None and not refresh_cache:
        if (datetime.now() - _LAST_REFRESH).seconds < 3600:
            return _PREFIX_CACHE

    prefix_ranges = []

    try:
        # First try to load from database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT start_range, end_range, provider 
                    FROM prefix_ranges 
                    ORDER BY start_range
                """)
                for row in cur.fetchall():
                    prefix_ranges.append({
                        "start": row[0],
                        "end": row[1],
                        "provider": row[2].strip().upper()
                    })
                logger.info(f"✅ Loaded {len(prefix_ranges)} prefix ranges from database")

        # If no ranges in database, fall back to CSV
        if not prefix_ranges:
            df = pd.read_csv("data/ranges.csv", delimiter=",")
            df.columns = df.columns.str.strip().str.lower()

            for _, row in df.iterrows():
                try:
                    start = int(row["start"])
                    end = int(row["end"])
                    provider = row["provider"].strip().upper()
                    prefix_ranges.append({"start": start, "end": end, "provider": provider})
                except Exception as e:
                    logger.warning(f"Skipping row in ranges.csv due to error: {e}")

            logger.info(f"✅ Loaded {len(prefix_ranges)} prefix ranges from ranges.csv")

        _PREFIX_CACHE = prefix_ranges
        _LAST_REFRESH = datetime.now()

    except Exception as e:
        logger.error(f"❌ Failed to load prefix ranges: {e}", exc_info=True)
        _PREFIX_CACHE = []

    return _PREFIX_CACHE

# ====================== MSISDN LOOKUP ======================
from Tools.eir_lookup import lookup_device_model

def get_msisdn_location(msisdn):
    try:
        conn = psycopg2.connect(
            dbname="tracedb",
            user="enet",
            password=os.environ.get("DB_PASSWORD", "changeme"),
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT tower_name, enodeb_id, cell_id, lat, lon, timestamp, source
            FROM latest_traces
            WHERE msisdn = %s
        """, (msisdn,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            return {
                "tower": result[0],
                "enodeb_id": result[1],
                "cell_id": result[2],
                "lat": result[3],
                "lon": result[4],
                "timestamp": result[5],
                "source": result[6],
            }
        else:
            return None

    except Exception as e:
        print(f"❌ PostgreSQL lookup failed: {e}")
        return None
                
# ====================== IMSI LOOKUP ======================

def get_imsi_location(imsi):
    try:
        conn = psycopg2.connect(
            dbname="tracedb",
            user="enet",
            password=os.environ.get("DB_PASSWORD", "changeme"),
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT tower_name, lat, lon, enodeb_id, cell_id, timestamp, msisdn
            FROM radius_matches
            WHERE imsi = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (imsi,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            return None

        return {
            "tower_name": row[0],
            "lat": row[1],
            "lon": row[2],
            "enodeb_id": row[3],
            "cell_id": row[4],
            "timestamp": row[5],
            "msisdn": row[6],
            "source": "trace_db"
        }

    except Exception as e:
        logger.error(f"❌ PostgreSQL IMSI lookup failed: {e}")
        return None
        

# ====================== DASHBOARD METRICS ======================
def generate_dashboard():
    try:
        prefix_map = load_prefix_map(refresh_cache=True)
        df = pd.read_excel("data/nmp_master.xlsx", dtype={"number": "str", "from": "str", "to": "str"}, parse_dates=["date"])
        df = df.dropna(subset=["number", "date"])
        df["number"] = df["number"].astype(str).str.strip().str.replace(".0", "", regex=False)
        df["prefix"] = df["number"].str[:3]
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].notna()]
        df["from"] = df["from"].str.upper()
        df["to"] = df["to"].str.upper()

        daily_stats = df.groupby(df["date"].dt.date).size().reset_index(name="total").to_dict("records")
        route_stats = df.groupby(["from", "to"]).size().reset_index(name="total").sort_values("total", ascending=False).to_dict("records")
        last_7_days = df[df["date"] >= datetime.now() - pd.Timedelta(days=7)].groupby("to").size().reset_index(name="count").sort_values("count", ascending=False).to_dict("records")
        prefix_blocks = [{"provider": k, "count": v} for k, v in defaultdict(int, {entry["provider"]: 0 for entry in prefix_map}).items()]
        provider_stats = df.groupby("to").size().reset_index(name="count").sort_values("count", ascending=False).to_dict("records")

        metadata = {
            "total_records": len(df),
            "generated_at": datetime.now().isoformat(),
            "date_range": {
                "min": df["date"].min().strftime("%Y-%m-%d") if not df.empty else None,
                "max": df["date"].max().strftime("%Y-%m-%d") if not df.empty else None,
            },
        }

        return {
            "daily": daily_stats,
            "routes": route_stats,
            "prefixes": prefix_blocks,
            "providers": provider_stats,
            "last_7_days": last_7_days,
            "metadata": metadata,
        }

    except Exception as e:
        logger.error(f"❌ Dashboard generation failed: {str(e)}")
        return {
            "error": str(e),
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "status": "failed",
            },
        }

# ====================== PROVIDER HELPER CLASS ======================
class ProviderHelper:
    """Handles provider code normalization and formatting"""

    @staticmethod
    def normalize(code):
        if not code:
            return "Unknown"
        code = str(code).upper()
        mapping = {
            "GTTG": "GTT",
            "ENTG": "ENet",
            "DIGG": "Digicel",
        }
        normalized = mapping.get(code, code)
        logger.debug(f"Normalized provider code {code} to {normalized}")
        return normalized

    @staticmethod
    def get_original(number):
        """Match a number against known prefix ranges to determine original provider"""
        global _PREFIX_CACHE
        try:
            if not _PREFIX_CACHE:
                _PREFIX_CACHE = load_prefix_map()

            number_int = int(str(number)[-7:])  # Last 7 digits only
            logger.debug(f"Checking prefix ranges for number: {number_int}")

            for entry in _PREFIX_CACHE:
                if entry["start"] <= number_int <= entry["end"]:
                    provider = ProviderHelper.normalize(entry["provider"])
                    logger.debug(f"Found matching prefix range: {entry}, provider: {provider}")
                    return provider

            logger.debug("No matching prefix range found")
            return ProviderHelper.normalize("Unknown")
        except Exception as e:
            logger.warning(f"Prefix range lookup failed for {number}: {e}")
            return ProviderHelper.normalize("Unknown")
