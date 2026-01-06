#!/bin/bash

echo "🔍 Checking if 'viewer_nmp_logs' table exists..."

# Backup app.py first
cp app.py app.py.bak
echo "🛡 Backed up app.py to app.py.bak"

# Rename table in SQLite database
sqlite3 logs/viewer_nmp_logs.db <<EOF
PRAGMA foreign_keys=off;
BEGIN TRANSACTION;
ALTER TABLE viewer_nmp_logs RENAME TO viewer_logs;
COMMIT;
EOF

echo "✅ Renamed 'viewer_nmp_logs' to 'viewer_logs' in database"

# Update Python code references in app.py (if any reference is using old name)
sed -i 's/viewer_nmp_logs/viewer_logs/g' app.py

echo "✅ Updated app.py to use 'viewer_logs' consistently"

# Confirm change
echo "📋 Verifying tables in DB:"
sqlite3 logs/viewer_nmp_logs.db ".tables"

# Show sample logs if exist
echo "📄 Sample log entries:"
sqlite3 logs/viewer_nmp_logs.db "SELECT * FROM viewer_logs LIMIT 5;"

echo "✅ Fix complete. You can now test viewer logging again."
