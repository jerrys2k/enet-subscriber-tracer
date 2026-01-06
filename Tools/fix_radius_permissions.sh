#!/bin/bash

# -----------------------------
# Grant user 'enet' access to FreeRADIUS logs
# -----------------------------

USER_TO_GRANT="enet"
RADIUS_LOG_DIR="/var/log/freeradius/radacct"
FREERAD_GROUP="freerad"

echo "📂 Checking RADIUS log directory: $RADIUS_LOG_DIR"

# 1. Add user to 'freerad' group (if not already)
if id -nG "$USER_TO_GRANT" | grep -qw "$FREERAD_GROUP"; then
    echo "✅ User '$USER_TO_GRANT' is already in group '$FREERAD_GROUP'"
else
    echo "➕ Adding '$USER_TO_GRANT' to group '$FREERAD_GROUP'"
    usermod -aG "$FREERAD_GROUP" "$USER_TO_GRANT"
    echo "⚠️ Please log out and log back in or run: newgrp $FREERAD_GROUP"
fi

# 2. Grant group read permissions recursively
echo "🔧 Setting group read access for FreeRADIUS detail files..."
find "$RADIUS_LOG_DIR" -type f -name "detail-*" -exec chmod g+r {} \;

# 3. Set group read permissions for all future files
echo "🔧 Ensuring directory has correct permissions..."
chmod -R g+rx "$RADIUS_LOG_DIR"

# 4. Optional: Change group ownership to 'freerad' recursively (if needed)
# echo "🔄 Resetting group ownership to $FREERAD_GROUP..."
# chgrp -R "$FREERAD_GROUP" "$RADIUS_LOG_DIR"

# 5. Optional: Persist default permissions using ACL
echo "🛠️ Applying default ACL to preserve group read permissions..."
apt-get install -y acl > /dev/null 2>&1
setfacl -R -m g::rX "$RADIUS_LOG_DIR"
setfacl -R -d -m g::rX "$RADIUS_LOG_DIR"

echo "✅ Permissions updated for user '$USER_TO_GRANT' to access FreeRADIUS logs."
