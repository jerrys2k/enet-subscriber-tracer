#!/bin/bash

# === Configuration ===
cd "$(dirname "$0")"
source .env

DATE=$(date +%Y%m%d)
SUMMARY_FILE="logs/summary_${USER}_${DATE}.json"
TODAY=$(date +%Y-%m-%d)

# === Count raw entries ===
raw_total=$(grep -c '^$' /var/log/freeradius/radacct/*/detail-${DATE}* 2>/dev/null | awk -F: '{sum += $2} END {print sum}')

# === Count inserted entries ===
inserted_today=$(psql -U "$PG_USER" -d "$PG_DB" -h "$PG_HOST" -t -c "
    SELECT COUNT(*) FROM radius_matches
    WHERE DATE(timestamp::timestamptz AT TIME ZONE 'UTC' AT TIME ZONE 'America/Guyana') = CURRENT_DATE;
" | tr -d '[:space:]')

# === Fetch recent backfill runs ===
recent_backfills=$(psql -U "$PG_USER" -d "$PG_DB" -h "$PG_HOST" -P format=unaligned -F '|' -t -c "
    SELECT filename, processed, inserted, to_char(backfill_time, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
    FROM backfill_audit
    ORDER BY backfill_time DESC
    LIMIT 2;
")

# === Build JSON ===
{
  echo "{"
  echo "  \"timestamp\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\","
  echo "  \"daily_status\": {"
  echo "    \"date\": \"${TODAY}\","
  echo "    \"total_raw_entries\": ${raw_total:-0},"
  echo "    \"inserted_today\": ${inserted_today:-0},"
  echo "    \"remaining_to_backfill\": $((raw_total - inserted_today))"
  echo "  },"
  echo "  \"last_backfill_runs\": ["
  echo "$recent_backfills" | awk -F'|' '{
    printf "    {\n      \"filename\": \"%s\",\n      \"processed\": %s,\n      \"inserted\": %s,\n      \"backfill_time\": \"%s\"\n    },\n", $1, $2, $3, $4
  }' | sed '$ s/},$/}/'
  echo "  ]"
  echo "}"
} > "$SUMMARY_FILE"

chmod 644 "$SUMMARY_FILE"
echo "✅ Summary exported to $SUMMARY_FILE"
