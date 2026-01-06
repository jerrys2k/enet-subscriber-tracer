# MSISDN Checker - Law Enforcement Trace Tool

## Overview
Real-time subscriber location tracking and device identification system for E-Networks Inc. (ENet), Guyana's national MNO. Used by law enforcement for subscriber tracing with full audit trails.

**URL:** http://100.64.253.252:7000/admin

## Architecture
```
┌─────────────────────────────────────────────────────────────────────┐
│                        MSISDN CHECKER                               │
├─────────────────────────────────────────────────────────────────────┤
│  Flask App (port 7000)                                              │
│  ├── /admin - Main dashboard                                        │
│  ├── /api/trace - API endpoints                                     │
│  └── /user-management - Admin portal                                │
├─────────────────────────────────────────────────────────────────────┤
│  Data Sources:                                                      │
│  ├── RADIUS logs → radius_watcher.service → PostgreSQL              │
│  ├── Sandvine Maestro → SSH CLI queries                             │
│  ├── EPT (Engineering Planning Tool) → Cell coordinates             │
│  ├── NMP (Number Portability) → Porting history                     │
│  └── Salesforce → Customer info                                     │
├─────────────────────────────────────────────────────────────────────┤
│  Database: PostgreSQL (tracedb)                                     │
│  ├── radius_matches (7M+ records) - Historical location + IMEI      │
│  ├── latest_traces (88K records) - Current subscriber location      │
│  ├── sandvine_sessions - Live session snapshots                     │
│  ├── viewer_logs - Audit trail                                      │
│  └── users - Authentication                                         │
└─────────────────────────────────────────────────────────────────────┘
```

## Features

### 1. MSISDN Lookup
- Real-time location from RADIUS + EPT enrichment
- Live session from Sandvine (IMSI, IMEI, IP, device)
- Customer info from Salesforce
- Porting history from NMP
- Interactive map with cell sector visualization

### 2. IMEI Search
- Search by device IMEI to find all MSISDNs that used it
- Data from RADIUS 3GPP-IMEISV field
- 10,000+ unique IMEIs indexed

### 3. IMSI Lookup
- Search by SIM card IMSI
- Historical location tracking

### 4. Audit Trail
- All lookups logged with timestamp, user, target
- Role-based access (superadmin, admin, viewer)

## Data Flow

### RADIUS Location Tracking
```
FreeRADIUS ─→ /var/log/freeradius/radacct/
                    │
                    ▼
           radius_watcher.service (polls every 5s)
                    │
                    ▼
           parse_radius_record() - extracts:
           - MSISDN (Calling-Station-Id)
           - IMSI (3GPP-IMSI)
           - IMEI (3GPP-IMEISV)
           - Cell (3GPP-User-Location-Info → decode_eci)
           - Timestamp
                    │
                    ▼
           PostgreSQL
           ├── latest_traces (upsert - current location)
           └── radius_matches (insert - history)
```

### 3GPP-User-Location-Info Decoding
```
Format: 0x82 + TAI(5 bytes) + ECGI(7 bytes)
Example: 0x823708401b5937084000103085
         │  │         │
         │  │         └── ECGI (MCC/MNC + ECI)
         │  └── TAI (MCC/MNC + TAC)
         └── Type (0x82 = TAI+ECGI)

ECI (last 8 hex chars): 00103085
  - eNodeB ID = (ECI >> 8) & 0xFFFFF = 4144
  - Cell ID = ECI & 0xFF = 133
```

### EPT Enrichment
```
eNodeB + Cell ID ─→ EPT lookup ─→ Tower name, Lat/Lon, Azimuth
                                        │
                                        ▼
                                  Display on map with sector cone
```

### Sandvine Live Session
```
MSISDN ─→ SSH to Sandvine Maestro (100.64.5.4:42002)
                    │
                    ▼
          show service subscriber-management 
          get-attribute-details name {msisdn} attribute-view Profile
                    │
                    ▼
          Returns: IP, IMSI, IMEI, eNodeB, Cell, Device, Session Duration
```

## Database Schema

### radius_matches
```sql
CREATE TABLE radius_matches (
    id SERIAL PRIMARY KEY,
    msisdn VARCHAR(15),
    imsi VARCHAR(15),
    imei VARCHAR(20),           -- From 3GPP-IMEISV
    enodeb_id VARCHAR(10),
    cell_id VARCHAR(10),
    tower_name VARCHAR(100),
    lat FLOAT,
    lon FLOAT,
    timestamp TIMESTAMP,
    UNIQUE(msisdn, enodeb_id, cell_id, timestamp)
);
CREATE INDEX idx_radius_msisdn ON radius_matches(msisdn);
CREATE INDEX idx_radius_imei ON radius_matches(imei);
CREATE INDEX idx_radius_timestamp ON radius_matches(timestamp);
```

### latest_traces
```sql
CREATE TABLE latest_traces (
    msisdn VARCHAR(15) PRIMARY KEY,
    imsi VARCHAR(15),
    enodeb_id VARCHAR(10),
    cell_id VARCHAR(10),
    tower_name VARCHAR(100),
    lat FLOAT,
    lon FLOAT,
    timestamp TIMESTAMP,
    source VARCHAR(20)  -- 'radius' or 'sandvine'
);
```

### sandvine_sessions
```sql
CREATE TABLE sandvine_sessions (
    id SERIAL PRIMARY KEY,
    msisdn VARCHAR(15),
    imsi VARCHAR(15),
    imei VARCHAR(20),
    ip_address VARCHAR(45),
    enodeb_id VARCHAR(10),
    cell_id VARCHAR(10),
    device_vendor VARCHAR(100),
    device_name VARCHAR(100),
    rat_type VARCHAR(20),
    timestamp TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_sandvine_imei ON sandvine_sessions(imei);
```

## Services

### msisdn_checker.service
```ini
[Unit]
Description=MSISDN Checker Flask App
After=network.target postgresql.service

[Service]
Type=simple
User=root
WorkingDirectory=/home/enet/msisdn_checker
ExecStart=/home/enet/msisdn_checker/venv/bin/gunicorn -w 4 -b 0.0.0.0:7000 app:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### radius_watcher.service
```ini
[Unit]
Description=RADIUS Log Watcher for MSISDN Checker
After=network.target postgresql.service

[Service]
Type=simple
User=root
WorkingDirectory=/home/enet/msisdn_checker
ExecStart=/home/enet/msisdn_checker/venv/bin/python3 /home/enet/msisdn_checker/Tools/radius_watcher.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Configuration

### RADIUS Sources
```python
RADIUS_DIRS = [
    "/var/log/freeradius/radacct/100.64.145.34",  # PGW 1
    "/var/log/freeradius/radacct/10.20.50.67"     # PGW 2
]
```

### Sandvine Connection
```python
HOST = "100.64.5.4"
PORT = 42002
USER = "admin"
PASSWORD = "password"
```

### Database
```python
DB_CONFIG = {
    "dbname": "tracedb",
    "user": "enet",
    "password": "${DB_PASSWORD}",
    "host": "localhost",
    "port": "5432"
}
```

## Known Issues & TODO

### Location Accuracy Issue (Investigating)
- RADIUS reports location from PGW Interim-Updates
- Updates only every ~30 minutes (PGW accounting interval)
- May show stale cell if subscriber moved but session didn't update
- **Potential fix:** Integrate with MME S1-AP for real-time handover tracking

### HSS Integration (Partial)
- HSS has MMEINFO but not detailed ECGI
- Commands tried:
  - `LST MMEINFO:ISDN="5926406900"` - Shows MME host, not cell
  - Need to find command for TAI/ECGI location

### TODO
- [ ] Investigate HSS commands for real-time cell location
- [ ] Consider MME/S1-AP integration for handover events
- [ ] Add location history trail visualization
- [ ] Implement real-time alerts (geofence, target online)
- [ ] Add CDR integration for call/SMS records
- [ ] HTTPS setup with nginx reverse proxy

## File Structure
```
/home/enet/msisdn_checker/
├── app.py                    # Main Flask application
├── users.db                  # SQLite user authentication
├── Tools/
│   ├── radius_parser.py      # RADIUS log parser + decode_eci
│   ├── radius_watcher.py     # Real-time RADIUS watcher service
│   ├── sandvine_client.py    # Sandvine Maestro SSH client
│   ├── ept_loader.py         # EPT cell data loader
│   ├── helpers.py            # Database helpers
│   └── salesforce_client.py  # Salesforce API client
├── templates/
│   └── admin/
│       └── dashboard.html    # Main UI template
├── data/
│   ├── ept_lte.pkl          # EPT LTE cell cache
│   ├── ept_nr.pkl           # EPT 5G NR cell cache
│   └── ept_meta.pkl         # EPT metadata
├── logs/
│   ├── app.log              # Application logs
│   └── radius_watcher.log   # Watcher service logs
└── README.md                # This file
```

## Quick Commands
```bash
# Restart services
sudo systemctl restart msisdn_checker.service
sudo systemctl restart radius_watcher.service

# Check status
sudo systemctl status msisdn_checker.service
sudo systemctl status radius_watcher.service

# View logs
tail -f logs/app.log
tail -f logs/radius_watcher.log

# Check database stats
PGPASSWORD='${DB_PASSWORD}' psql -h localhost -U enet -d tracedb -c "
SELECT 'radius_matches' as tbl, COUNT(*) FROM radius_matches
UNION ALL SELECT 'latest_traces', COUNT(*) FROM latest_traces
UNION ALL SELECT 'sandvine_sessions', COUNT(*) FROM sandvine_sessions;"

# Check unique IMEIs
PGPASSWORD='${DB_PASSWORD}' psql -h localhost -U enet -d tracedb -c "
SELECT COUNT(DISTINCT imei) FROM radius_matches WHERE imei != '';"

# Decode a 3GPP-User-Location-Info hex
python3 -c "
hex='0x823708401b5937084000103085'
h=hex.replace('0x','')
eci=int(h[-8:],16)
print(f'eNodeB: {(eci>>8)&0xFFFFF}, Cell: {eci&0xFF}')
"

# Manual EPT lookup
python3 -c "
import sys; sys.path.insert(0,'.')
from Tools.ept_loader import get_cell_details
print(get_cell_details('4144','133'))
"
```

## Contact
Gerald Singh - CTO, E-Networks Inc.
