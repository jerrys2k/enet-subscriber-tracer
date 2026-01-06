#!/bin/bash

set -e

echo "🔧 Adding 'enet' to the 'freerad' group..."
usermod -aG freerad enet

echo "🔐 Enabling group inheritance on /var/log/freeradius/radacct..."
chown root:freerad /var/log/freeradius/radacct
chmod g+s /var/log/freeradius/radacct

echo "🧼 Fixing permissions on existing log files..."
chmod -R g+rX /var/log/freeradius/radacct

echo "⏱️ Installing fallback cron job to fix permissions every 5 minutes..."
cat <<EOF > /etc/cron.d/radius-log-fix
*/5 * * * * root chmod -R g+rX /var/log/freeradius/radacct
EOF

echo "✅ Permissions patched. Please log out and log back in for group changes to take effect."
