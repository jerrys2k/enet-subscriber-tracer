#!/bin/bash

echo "🔧 Setting up optional enhancements for MSISDN checker..."

# 1. Ensure latest_traces enhancements in PostgreSQL
sudo -u postgres psql -d tracedb <<EOF
CREATE INDEX IF NOT EXISTS idx_latest_traces_msisdn ON latest_traces(msisdn);
ALTER TABLE latest_traces ALTER COLUMN source SET DEFAULT 'live';
DO \$\$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name='latest_traces' AND column_name='updated_at'
    ) THEN
        ALTER TABLE latest_traces ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();
    END IF;
END
\$\$ LANGUAGE plpgsql;
EOF

# 2. Ensure logs directory exists
mkdir -p /home/enet/msisdn_checker/logs

# 3. Create systemd service for radius_watcher
cat <<EOL | sudo tee /etc/systemd/system/radius_watcher.service > /dev/null
[Unit]
Description=Live Radius Log Watcher
After=network.target

[Service]
Type=simple
User=enet
WorkingDirectory=/home/enet/msisdn_checker
ExecStart=/home/enet/msisdn_checker/venv/bin/python3 Tools/radius_watcher_live.py
Restart=always
RestartSec=5
StandardOutput=append:/home/enet/msisdn_checker/logs/radius_watcher.log
StandardError=append:/home/enet/msisdn_checker/logs/radius_watcher_error.log

[Install]
WantedBy=multi-user.target
EOL

# 4. Reload and enable the systemd service
sudo systemctl daemon-reexec
sudo systemctl enable radius_watcher.service
sudo systemctl restart radius_watcher.service

echo "✅ Enhancements applied and radius_watcher.service started!"
