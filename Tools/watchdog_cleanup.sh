#!/bin/bash
# ====================================================
# 🧠 MSISDN Backfill Lockfile Watchdog Cleaner
# Automatically removes stale lockfile if the PID is dead
# or if lock file is older than 2 hours
# ====================================================

LOCK_FILE="/tmp/msisdn_backfill.lock"
TIMEOUT=7200  # seconds = 2 hours

if [ -f "$LOCK_FILE" ]; then
    PID=$(cat "$LOCK_FILE")
    
    if [ -n "$PID" ] && ! ps -p "$PID" > /dev/null; then
        echo "$(date +"%Y-%m-%d %H:%M:%S") | 🧹 No process for PID $PID — removing stale lock"
        rm -f "$LOCK_FILE"
    else
        AGE=$(($(date +%s) - $(stat -c %Y "$LOCK_FILE")))
        if [ "$AGE" -gt "$TIMEOUT" ]; then
            echo "$(date +"%Y-%m-%d %H:%M:%S") | ⏱️ Lock older than $((TIMEOUT/60)) min — removing"
            rm -f "$LOCK_FILE"
        fi
    fi
fi
