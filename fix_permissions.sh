#!/bin/bash

# === MSISDN CHECKER PERMISSIONS FIXER ===
# Ensures proper access for enet and freerad users

APP_DIR="/home/enet/msisdn_checker"
LOG_DIR="$APP_DIR/logs"
DATA_DIR="$APP_DIR/data"
TOOLS_DIR="$APP_DIR/Tools"
PROGRESS_FILE="$LOG_DIR/backfill_progress.txt"

ENET_USER="enet"
FREERAD_GROUP="freerad"

echo "🔧 Fixing ownership and permissions in $APP_DIR..."

# Ensure main folders exist
mkdir -p "$LOG_DIR" "$DATA_DIR" "$TOOLS_DIR"

# 1. Make enet user own everything by default
chown -R $ENET_USER:$ENET_USER "$APP_DIR"

# 2. Allow freerad group to write to logs and progress
chgrp -R $FREERAD_GROUP "$LOG_DIR" "$DATA_DIR"
chmod -R 775 "$LOG_DIR" "$DATA_DIR"

# 3. Specific resume file used by freerad
touch "$PROGRESS_FILE"
chown $ENET_USER:$FREERAD_GROUP "$PROGRESS_FILE"
chmod 664 "$PROGRESS_FILE"

# 4. Ensure Python scripts are executable
chmod +x "$APP_DIR"/check_backfill_status.sh
chmod +x "$APP_DIR"/venv/bin/python

echo "✅ Permissions fixed. enet owns everything. freerad can write to logs + progress."
