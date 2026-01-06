import os
import time
import subprocess
import threading
from datetime import datetime

# 🕒 Automatically use today's RADIUS log files
today = datetime.now().strftime("%Y%m%d")

WATCH_PATHS = [
    f"/var/log/freeradius/radacct/100.64.145.34/detail-{today}",
    f"/var/log/freeradius/radacct/10.20.50.67/detail-{today}"
]

# 🧠 Your parser script that decodes and inserts into latest_traces
PARSER = "Tools/parse_radius_logs_debug.py"

def follow_lines(path):
    print(f"[LIVE] Watching {path}")
    if not os.path.exists(path):
        print(f"[ERROR] File not found: {path}")
        return

    with open(path, 'r') as file:
        file.seek(0, os.SEEK_END)  # Jump to end of file
        buffer = []
        while True:
            line = file.readline()
            if not line:
                time.sleep(0.2)
                continue

            buffer.append(line)
            if line.strip() == "":
                # Write to temp file and call parser
                with open("/tmp/live_radius_block", "w") as temp:
                    temp.writelines(buffer)

                subprocess.run(["python3", PARSER, "/tmp/live_radius_block"])
                time.sleep(0.3)  # prevent CPU overuse
                buffer = []

def main():
    threads = []
    for path in WATCH_PATHS:
        t = threading.Thread(target=follow_lines, args=(path,))
        t.daemon = True
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
