#!/bin/bash

# =============================================
# MSISDN Backfill Status Check
# Version: 2.3
# Last Updated: 2025-05-29
# =============================================

set -o errexit -o nounset -o pipefail

# ----------------------------
# Configuration
# ----------------------------
readonly VERSION="2.3"
readonly DEFAULT_LOG_DIR="logs"
readonly ENV_FILE=".env"
readonly SUMMARY_JSON_PREFIX="summary"
readonly TODAY=$(date +%Y%m%d)
readonly TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Colors for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[0;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m' # No Color

# ----------------------------
# Initialize Variables
# ----------------------------
LOG_DIR="${LOG_DIR:-$DEFAULT_LOG_DIR}"
LOG_FILE="/dev/null"  # Initialize, will be set in init_dirs
SUMMARY_JSON="/dev/null"

# ----------------------------
# Functions
# ----------------------------

# Safe logging function
log() {
    local level="$1"
    local message="$2"
    local color="$NC"
    
    case "$level" in
        "ERROR") color="$RED" ;;
        "SUCCESS") color="$GREEN" ;;
        "WARNING") color="$YELLOW" ;;
        "INFO") color="$BLUE" ;;
    esac
    
    # Try to log to file if configured
    if [ "$LOG_FILE" != "/dev/null" ]; then
        echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $message" >> "$LOG_FILE" 2>/dev/null || true
    fi
    
    # Always show to console
    echo -e "${color}[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $message${NC}" >&2
}

# Initialize directories with proper permissions
init_dirs() {
    # First try the requested log directory
    if [ ! -d "$LOG_DIR" ]; then
        if ! mkdir -p "$LOG_DIR" 2>/dev/null; then
            log "WARNING" "Cannot create log directory $LOG_DIR, using /tmp"
            LOG_DIR="/tmp/backfill_logs_${USER}"
            mkdir -p "$LOG_DIR"
        fi
    fi

    # Verify we can write to the directory
    if ! touch "$LOG_DIR/test.tmp" 2>/dev/null; then
        log "WARNING" "Cannot write to $LOG_DIR, logging to console only"
        LOG_FILE="/dev/null"
        SUMMARY_JSON="/dev/null"
    else
        rm -f "$LOG_DIR/test.tmp"
        LOG_FILE="$LOG_DIR/backfill_${TODAY}.log"
        SUMMARY_JSON="$LOG_DIR/${SUMMARY_JSON_PREFIX}_${USER}_${TODAY}.json"
    fi
}

# Validate required environment variables
validate_env() {
    log "INFO" "Validating environment configuration..."
    
    if [ ! -f "$ENV_FILE" ]; then
        log "ERROR" "❌ .env file not found"
        exit 1
    fi

    # Load environment safely
    set -a
    source "$ENV_FILE"
    set +a

    local required_vars=("PG_PASSWORD" "PG_USER" "PG_DB" "PG_HOST")
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            log "ERROR" "❌ Required variable $var not set in .env"
            exit 1
        fi
    done

    export PGCONN="postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}/${PG_DB}"
    export PG_OPTS="--no-password --quiet -A -F|"
}

# Check/migrate audit table
setup_audit_table() {
    log "INFO" "Checking/migrating audit table structure..."
    
    local query=$(cat <<EOF
DO \$\$
BEGIN
    CREATE TABLE IF NOT EXISTS backfill_audit (
        filename TEXT PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'complete',
        last_run TIMESTAMPTZ DEFAULT now(),
        processed INT DEFAULT 0,
        inserted INT DEFAULT 0,
        deduplicated INT DEFAULT 0,
        runtime_seconds INT DEFAULT 0
    );
END
\$\$;
EOF
    )

    if ! psql "$PGCONN" -v ON_ERROR_STOP=1 -c "$query" >/dev/null; then
        log "ERROR" "❌ Failed to setup audit table"
        exit 1
    fi
}

# Check active processes
check_process_status() {
    log "INFO" "Checking active backfill processes..."
    
    local processes=$(ps -eo pid,etime,user,cmd | grep backfill_radius_history.py | grep -v grep)
    
    if [[ -z "$processes" ]]; then
        log "SUCCESS" "✔ No active backfill processes found"
    else
        log "WARNING" "⚠ Active backfill processes detected:"
        while IFS= read -r line; do
            pid=$(echo "$line" | awk '{print $1}')
            runtime=$(echo "$line" | awk '{print $2}')
            user=$(echo "$line" | awk '{print $3}')
            printf "  PID: %-8s Runtime: %-12s User: %s\n" "$pid" "$runtime" "$user"
        done <<< "$processes"
    fi
}

# Get today's inserted count
get_todays_inserts() {
    log "INFO" "Calculating today's inserts..."
    
    local query_guyana=$(cat <<EOF
SELECT COUNT(*) FROM radius_matches
WHERE DATE(timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/Guyana') = CURRENT_DATE;
EOF
    )

    local query_utc=$(cat <<EOF
SELECT COUNT(*) FROM radius_matches
WHERE DATE(timestamp) = CURRENT_DATE;
EOF
    )

    local inserted_today=$(timeout 5 psql "$PGCONN" $PG_OPTS -c "$query_guyana" 2>/dev/null | tr -d '[:space:]|')
    
    if [[ -z "$inserted_today" ]]; then
        inserted_today=$(timeout 5 psql "$PGCONN" $PG_OPTS -c "$query_utc" 2>/dev/null | tr -d '[:space:]|')
        log "INFO" "Today's inserts (UTC): $inserted_today"
    else
        log "INFO" "Today's inserts (Guyana time): $inserted_today"
    fi
    
    echo "$inserted_today"
}

# Get recent audit summary
get_audit_summary() {
    log "INFO" "Fetching recent audit summary..."
    
    local query=$(cat <<EOF
SELECT 
    filename, 
    COALESCE(processed, 0) as processed, 
    COALESCE(inserted, 0) as inserted,
    TO_CHAR(last_run, 'YYYY-MM-DD HH24:MI') AS time
FROM backfill_audit
ORDER BY last_run DESC
LIMIT 5;
EOF
    )

    local results
    results=$(psql "$PGCONN" -P pager=off -F $'\t' -A -c "$query" 2>/dev/null)
    
    if [[ -z "$results" ]]; then
        echo "  No audit entries found"
    else
        while IFS= read -r line; do
            filename=$(echo "$line" | cut -f1)
            processed=$(echo "$line" | cut -f2)
            inserted=$(echo "$line" | cut -f3)
            time=$(echo "$line" | cut -f4)
            printf "  %-60s %8s processed %8s inserted at %s\n" "$filename" "$processed" "$inserted" "$time"
        done <<< "$results"
    fi
}

# Count total RADIUS entries
count_radius_entries() {
    log "INFO" "Counting RADIUS entries in today's files..."
    
    local total_entries=$(grep -c '^$' /var/log/freeradius/radacct/*/detail-$TODAY* 2>/dev/null | awk -F: '{sum += $2} END {print sum}')
    
    if [[ -n "$total_entries" ]]; then
        log "INFO" "Total entries found: $total_entries"
    else
        log "WARNING" "No files found or read access denied"
        total_entries=0
    fi
    
    echo "$total_entries"
}

# Get per-PGW summary
get_pgw_summary() {
    log "INFO" "Generating per-PGW summary..."
    
    local query=$(cat <<EOF
SELECT 
    SUBSTRING(filename FROM '/var/log/freeradius/radacct/([0-9.]+)/') AS pgw_ip,
    SUM(processed) as scanned,
    SUM(inserted) as inserted
FROM backfill_audit
WHERE DATE(last_run) = CURRENT_DATE
GROUP BY pgw_ip
ORDER BY pgw_ip;
EOF
    )

    local results
    results=$(psql "$PGCONN" -P pager=off -F $'\t' -A -c "$query" 2>/dev/null)
    
    if [[ -z "$results" ]]; then
        echo "  No PGW data available"
    else
        while IFS= read -r line; do
            pgw_ip=$(echo "$line" | cut -f1)
            scanned=$(echo "$line" | cut -f2)
            inserted=$(echo "$line" | cut -f3)
            printf "  🌐 PGW %-15s: %8s scanned %8s inserted\n" "$pgw_ip" "$scanned" "$inserted"
        done <<< "$results"
    fi
}

# Check for stale processes
check_stale_processes() {
    log "INFO" "Checking for stale processes..."
    
    local stale_procs=$(ps -eo pid,etime,user,cmd | grep backfill_radius_history.py | grep -v grep | awk '$2 ~ /-/')
    
    if [[ -n "$stale_procs" ]]; then
        log "WARNING" "⚠ Stale processes detected (running >24h):"
        while IFS= read -r line; do
            pid=$(echo "$line" | awk '{print $1}')
            runtime=$(echo "$line" | awk '{print $2}')
            printf "  PID %s running for %s\n" "$pid" "$runtime"
        done <<< "$stale_procs"
    else
        log "SUCCESS" "✔ No stale processes found"
    fi
}

# Generate JSON summary
generate_json_summary() {
    log "INFO" "Generating JSON summary..."
    
    local inserted_today=$1
    local total_entries=$2
    
    if [ "$SUMMARY_JSON" = "/dev/null" ]; then
        log "WARNING" "Cannot save summary - no writable log directory"
        return
    fi
    
    cat > "$SUMMARY_JSON" <<EOF
{
  "metadata": {
    "script": "backfill_status.sh",
    "version": "$VERSION",
    "timestamp": "$TIMESTAMP",
    "log_dir": "$LOG_DIR"
  },
  "stats": {
    "inserted_today": "$inserted_today",
    "total_entries": "$total_entries",
    "active_pids": [$(pgrep -f backfill_radius_history.py | paste -sd "," -)]
  },
  "latest_logs": [
$(if [ -f "$LOG_FILE" ] && [ "$LOG_FILE" != "/dev/null" ]; then
    tail -n 5 "$LOG_FILE" | sed 's/"/\\"/g' | awk '{print "    \"" $0 "\","}' | sed '$ s/,$//'
else
    echo "    \"No log file available\""
fi)
  ]
}
EOF

    if [ -f "$SUMMARY_JSON" ]; then
        log "SUCCESS" "✔ Summary saved to $SUMMARY_JSON"
    else
        log "ERROR" "Failed to save summary to $SUMMARY_JSON"
    fi
}

# ----------------------------
# Main Execution
# ----------------------------

main() {
    echo -e "${BLUE}📌 MSISDN Backfill Status Check @ $(date)${NC}"
    echo -e "${BLUE}---------------------------------------------${NC}"
    
    init_dirs
    validate_env
    setup_audit_table
    
    echo -e "\n${YELLOW}🔍 Process Status:${NC}"
    check_process_status
    
    local inserted_today=$(get_todays_inserts)
    local total_entries=$(count_radius_entries)
    
    echo -e "\n${YELLOW}📄 Recent Audit Entries:${NC}"
    get_audit_summary
    
    echo -e "\n${YELLOW}📊 Per-PGW Summary for $(date +%Y-%m-%d):${NC}"
    get_pgw_summary
    
    echo -e "\n${YELLOW}📈 Total inserted today:${NC} $inserted_today"
    echo -e "${YELLOW}📦 Total entries awaiting backfill:${NC} $total_entries"
    
    check_stale_processes
    generate_json_summary "$inserted_today" "$total_entries"
    
    echo -e "\n${GREEN}✅ Status check completed successfully${NC}"
}

main "$@"
