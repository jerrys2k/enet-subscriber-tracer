import os
import re
from collections import deque

# Configuration
LOG_DIR = "/var/log/freeradius/radacct"
DETAIL_PREFIX = "detail-"
ENTRY_LIMIT = 8000

# Regex for parsing
entry_separator = re.compile(r"^\s*$")
field_pattern = re.compile(r'(\S+)\s+=\s+"?([^"]+)"?')


def get_latest_detail_file():
    """Find the most recent RADIUS detail-* log file."""
    log_files = []
    for root, _, files in os.walk(LOG_DIR):
        for name in files:
            if name.startswith(DETAIL_PREFIX):
                log_files.append(os.path.join(root, name))
    return max(log_files, key=os.path.getmtime) if log_files else None


def parse_logs(limit=ENTRY_LIMIT):
    """
    Parse the latest RADIUS detail log file (limited to last N lines).
    
    Returns:
        List[Dict]: Parsed RADIUS AVP entries
    """
    log_file = get_latest_detail_file()
    if not log_file:
        print("❌ No RADIUS detail log file found.")
        return []

    entries = []
    current = {}

    try:
        with open(log_file) as f:
            lines = deque(f, maxlen=limit)

        for line in lines:
            line = line.strip()
            if not line:
                if current:
                    entries.append(current)
                    current = {}
                continue

            match = field_pattern.match(line)
            if match:
                key, value = match.groups()
                current[key] = value

        if current:
            entries.append(current)

        return entries

    except Exception as e:
        print(f"❌ Error parsing {log_file}: {e}")
        return []
