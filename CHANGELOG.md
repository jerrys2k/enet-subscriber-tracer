# Changelog

## 2026-01-06 - Major Update

### Added
- **RADIUS Watcher v2** - Efficient polling-based watcher replacing watchdog
  - Processes ~1,200 records every 5 seconds
  - Proper IMEI capture from 3GPP-IMEISV
  - Batch inserts for performance

- **IMEI Search** - Search by device IMEI
  - Queries radius_matches table
  - Shows all MSISDNs that used the device
  - 10,000+ unique IMEIs indexed

- **EPT Enrichment for RADIUS** - Offline subscribers now show coordinates
  - Previously only Sandvine (online) subscribers had coordinates
  - Now RADIUS-only lookups also show tower location

- **Modern UI** - Three-tab search interface
  - MSISDN tab (phone number)
  - IMEI tab (device)
  - IMSI tab (SIM card)

- **Security Hardening**
  - Strong random secret key
  - Session security (HTTPOnly, SameSite)
  - Password hashing (scrypt)
  - Account lockout

### Fixed
- Duplicate watcher processes
- Missing EPT enrichment for offline subscribers
- IMEI not being captured in RADIUS records

### Investigated
- Location accuracy issue (RADIUS shows ~30min stale data)
- HSS integration for real-time cell (commands not returning ECGI)

### Database Stats (as of 2026-01-06 14:55)
- radius_matches: 7M+ records
- latest_traces: 88K+ records
- Unique IMEIs: 10,000+
- EPT cells: 3,378 LTE + 302 5G NR
