#!/bin/bash

# === Create logs directory if missing ===
mkdir -p /home/enet/msisdn_checker/logs

# === Make all scripts executable ===
chmod +x /home/enet/msisdn_checker/Tools/*.sh

# === Install user crontab ===
crontab - <<EOF
*/30 * * * * /home/enet/msisdn_checker/Tools/run_backfill.sh >> /home/enet/msisdn_checker/logs/backfill.log 2>&1
*/5 * * * * cd /home/enet/msisdn_checker && PYTHONPATH=. /home/enet/msisdn_checker/venv/bin/python Tools/trace_incremental.py >> logs/incremental.log 2>&1
@weekly /home/enet/msisdn_checker/venv/bin/python /home/enet/msisdn_checker/Tools/prune_old_traces.py >> /home/enet/msisdn_checker/logs/prune.log 2>&1
59 23 * * * /home/enet/msisdn_checker/venv/bin/python /home/enet/msisdn_checker/Tools/backfill_summary.py >> /home/enet/msisdn_checker/logs/summary.log 2>&1
*/10 * * * * /home/enet/msisdn_checker/Tools/watchdog_cleanup.sh >> /home/enet/msisdn_checker/logs/watchdog.log 2>&1
0 */6 * * * /home/enet/msisdn_checker/auto_git_backup.sh >> /home/enet/msisdn_checker/logs/git_backup.log 2>&1
EOF

echo "✅ Crontab installed and all scripts prepared."
