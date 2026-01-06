#!/bin/bash

# ===================== ENVIRONMENT =====================
export PYTHONPATH=/home/enet/msisdn_checker
cd /home/enet/msisdn_checker || exit 1

# ===================== ACTIVATE VENV =====================
source venv/bin/activate

# ===================== ENSURE LOG FOLDER EXISTS =====================
mkdir -p logs

# ===================== TIMESTAMP HEADER =====================
echo "$(TZ='America/Guyana' date '+%Y-%m-%d %H:%M:%S %Z') | 🔁 Starting PostgreSQL backfill" >> logs/backfill.log

# ===================== EXECUTE BACKFILL =====================
venv/bin/python Tools/backfill_radius_history.py >> logs/backfill.log 2>&1

# ===================== TIMESTAMP FOOTER =====================
echo "$(TZ='America/Guyana' date '+%Y-%m-%d %H:%M:%S %Z') | ✅ Backfill job complete" >> logs/backfill.log
