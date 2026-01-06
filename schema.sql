-- ===================== Table: radius_matches =====================
CREATE TABLE IF NOT EXISTS radius_matches (
    id SERIAL PRIMARY KEY,
    msisdn TEXT NOT NULL,
    imsi TEXT,
    enodeb_id INTEGER,
    cell_id INTEGER,
    tower_name TEXT,
    lat REAL,
    lon REAL,
    timestamp TEXT
);

-- ===================== Table: latest_traces =====================
CREATE TABLE IF NOT EXISTS latest_traces (
    msisdn TEXT PRIMARY KEY,
    imsi TEXT,
    enodeb_id INTEGER,
    cell_id INTEGER,
    tower_name TEXT,
    lat REAL,
    lon REAL,
    timestamp TEXT,
    source TEXT,
    device_model TEXT
);
