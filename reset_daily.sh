#!/bin/bash

echo "🧹 Starting daily RADIUS + DB reset..."

# 1. Clear old RADIUS logs
echo "🧾 Clearing RADIUS logs from /var/log/freeradius/radacct/"
rm -rf /var/log/freeradius/radacct/*

# 2. Reset the trace database tables
echo "🗑 Wiping radius_matches and latest_traces..."
sqlite3 data/trace_db.sqlite <<EOF
DELETE FROM radius_matches;
DELETE FROM latest_traces;
EOF

# 3. Reset timestamp file to today's midnight (GMT-4)
echo "⏳ Resetting timestamp to midnight GMT-4"
date -d 'today 00:00 -0400' +%s > data/.last_parsed_timestamp

echo "✅ Daily reset complete."
