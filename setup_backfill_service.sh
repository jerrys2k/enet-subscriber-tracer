#!/bin/bash

set -e

echo "🔍 Scanning crontabs for conflicting jobs..."

# Look for anything related to backfill or MSISDN
PATTERNS="backfill_radius_history.py|check_backfill_status.sh|radius_matches"
for user in $(cut -f1 -d: /etc/passwd); do
    if crontab -l -u "$user" 2>/dev/null | grep -E "$PATTERNS"; then
        echo "⚠️ Found cron jobs for user $user related to backfill:"
        crontab -l -u "$user" 2>/dev/null | grep -E "$PATTERNS"
        
        read -p "❌ Remove these cron jobs for user $user? [y/N]: " confirm
        if [[ "$confirm" =~ ^[Yy]$ ]]; then
            crontab -l -u "$user" 2>/dev/null | grep -vE "$PATTERNS" | crontab -u "$user" -
            echo "✅ Removed."
        fi
    fi
done

echo ""
echo "🧹 Removing old systemd units (if any)..."
sudo systemctl disable --now msisdn_backfill.service msisdn_backfill.timer 2>/dev/null || true
sudo rm -f /etc/systemd/system/msisdn_backfill.service /etc/systemd/system/msisdn_backfill.timer

echo ""
echo "🛠️ Creating new systemd service and timer..."

# Service unit
sudo tee /etc/systemd/system/msisdn_backfill.service > /dev/null <<EOF
[Unit]
Description=MSISDN RADIUS Backfill
After=network.target

[Service]
Type=simple
WorkingDirectory=/home/enet/msisdn_checker
ExecStart=/home/enet/msisdn_checker/venv/bin/python Tools/backfill_radius_history.py
User=freerad
Restart=no
EOF

# Timer unit
sudo tee /etc/systemd/system/msisdn_backfill.timer > /dev/null <<EOF
[Unit]
Description=Daily MSISDN Backfill at 2:15 AM

[Timer]
OnCalendar=*-*-* 02:15:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

echo ""
echo "🔄 Reloading systemd and enabling the timer..."
sudo systemctl daemon-reexec
sudo systemctl daemon-reload
sudo systemctl enable --now msisdn_backfill.timer

echo ""
echo "✅ Setup complete. The RADIUS backfill will now run automatically every day at 2:15 AM."
echo "You can check the status anytime with:"
echo "  systemctl status msisdn_backfill.timer"
echo "  journalctl -u msisdn_backfill.service"
