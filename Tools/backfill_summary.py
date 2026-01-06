import os
from datetime import datetime

logfile = "logs/backfill.log"
summary_file = "logs/daily_summary.log"

with open(logfile, "r") as f:
    lines = f.readlines()

today = datetime.now().strftime("%b %d")
summary = [line for line in lines if today in line]

with open(summary_file, "a") as out:
    out.write(f"\n===== Summary for {today} =====\n")
    out.writelines(summary[-50:])  # Last 50 relevant lines
