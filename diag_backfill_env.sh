#!/bin/bash

# Run as root or with sudo
set -e

APP_DIR="/home/enet/msisdn_checker"
LOG_DIR="$APP_DIR/logs"
EXCEL_FILE="$APP_DIR/data/E_Networks_EPT_2025APR25.xlsx"
PYTHON_BIN="$APP_DIR/venv/bin/python"
BACKFILL_SCRIPT="$APP_DIR/Tools/backfill_radius_history.py"

echo "📍 Ensuring log and data directories are accessible to 'freerad'"
chown -R freerad:freerad "$LOG_DIR"
chmod -R 755 "$LOG_DIR"

if [ ! -f "$EXCEL_FILE" ]; then
  echo "❌ Excel mapping file not found: $EXCEL_FILE"
  exit 1
fi

chown freerad:freerad "$EXCEL_FILE"
chmod 644 "$EXCEL_FILE"

echo "🔎 Current permissions:"
ls -ld "$LOG_DIR"
ls -l "$EXCEL_FILE"

echo "🔁 Testing execution of backfill script as freerad..."
sudo -u freerad -H bash <<EOF
echo "👤 User: \$(whoami)"
cd "$APP_DIR"
echo "📂 Directory: \$(pwd)"
echo "🏠 Home: \$HOME"
echo "🔧 PYTHONPATH and running script..."

PYTHONPATH=. $PYTHON_BIN $BACKFILL_SCRIPT
EOF

echo "✅ Backfill test completed."

