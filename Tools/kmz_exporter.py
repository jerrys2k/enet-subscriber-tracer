#!/usr/bin/env python3
"""
KMZ Exporter Module
- Export tower locations to KMZ format
- PostgreSQL integration
- Enhanced logging
"""

import os
import logging
import psycopg2
from datetime import datetime, timezone
import simplekml
from dotenv import load_dotenv

load_dotenv()

# ====================== CONFIGURATION ======================
class Config:
    DB_CONFIG = {
        "dbname": "tracedb",
        "user": "enet",
        "password": "${DB_PASSWORD}",
        "host": "localhost",
        "port": "5432"
    }
    LOG_FILE = "logs/kmz_export.log"
    MAX_RETRIES = 3
    RETRY_DELAY = 1
    OUTPUT_DIR = "exports"

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

def get_tower_locations():
    """Retrieve tower locations from the database"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT tower_name, lat, lon, enodeb_id, cell_id
                    FROM latest_traces
                    WHERE lat IS NOT NULL AND lon IS NOT NULL
                    ORDER BY tower_name
                """)
                return cur.fetchall()
    except Exception as e:
        logger.error(f"Failed to retrieve tower locations: {e}")
        return []

def get_tower_traces(tower_name):
    """Retrieve recent traces for a specific tower"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT msisdn, timestamp, device_model
                    FROM latest_traces
                    WHERE tower_name = %s
                    ORDER BY timestamp DESC
                    LIMIT 10
                """, (tower_name,))
                return cur.fetchall()
    except Exception as e:
        logger.error(f"Failed to retrieve tower traces: {e}")
        return []

# ====================== KMZ GENERATION ======================
def create_sector(lat, lon, azimuth, beamwidth, radius_km):
    """Create a sector polygon for a tower"""
    import math
    
    # Convert radius from km to degrees (approximate)
    radius_deg = radius_km / 111.32
    
    # Calculate sector points
    points = []
    start_angle = azimuth - beamwidth/2
    end_angle = azimuth + beamwidth/2
    
    # Add center point
    points.append((lon, lat))
    
    # Add sector points
    for angle in range(int(start_angle), int(end_angle) + 1):
        rad = math.radians(angle)
        dx = radius_deg * math.sin(rad)
        dy = radius_deg * math.cos(rad)
        points.append((lon + dx, lat + dy))
    
    return points

def generate_kmz(output_file=None):
    """Generate KMZ file with tower locations and coverage"""
    try:
        # Create output directory
        os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
        
        # Generate output filename
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(Config.OUTPUT_DIR, f"towers_{timestamp}.kmz")
        
        # Create KML document
        kml = simplekml.Kml()
        
        # Get tower locations
        towers = get_tower_locations()
        logger.info(f"Found {len(towers)} towers to export")
        
        # Create tower points and sectors
        for tower in towers:
            tower_name, lat, lon, enodeb_id, cell_id = tower
            
            # Create tower point
            point = kml.newpoint(name=tower_name)
            point.coords = [(lon, lat)]
            point.style.iconstyle.icon.href = "http://maps.google.com/mapfiles/kml/shapes/tower.png"
            point.style.iconstyle.scale = 1.0
            point.style.labelstyle.scale = 0.8
            
            # Get recent traces
            traces = get_tower_traces(tower_name)
            
            # Create description
            desc = f"<h3>{tower_name}</h3>"
            desc += f"<p>eNB ID: {enodeb_id}<br>Cell ID: {cell_id}</p>"
            if traces:
                desc += "<h4>Recent Traces:</h4><ul>"
                for msisdn, timestamp, device in traces:
                    desc += f"<li>{msisdn} ({device}) - {timestamp}</li>"
                desc += "</ul>"
            point.description = desc
            
            # Create coverage sectors (3 sectors per tower)
            for i, azimuth in enumerate([0, 120, 240]):
                sector = kml.newpolygon(name=f"{tower_name} Sector {i+1}")
                sector.outerboundaryis = create_sector(lat, lon, azimuth, 120, 2.0)
                sector.style.polystyle.color = simplekml.Color.blue
                sector.style.polystyle.fill = 1
                sector.style.polystyle.outline = 1
                sector.style.linestyle.color = simplekml.Color.blue
                sector.style.linestyle.width = 2
        
        # Save KMZ file
        kml.savekmz(output_file)
        logger.info(f"KMZ file generated: {output_file}")
        return output_file
        
    except Exception as e:
        logger.error(f"Failed to generate KMZ file: {e}")
        raise

# ====================== ENTRY POINT ======================
if __name__ == "__main__":
    try:
        output_file = generate_kmz()
        print(f"KMZ file generated: {output_file}")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
