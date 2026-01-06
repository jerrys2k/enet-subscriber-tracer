
#!/bin/bash

APP_FILE="app.py"
BACKUP_FILE="app.py.bak"

echo "Backing up original $APP_FILE to $BACKUP_FILE..."
cp $APP_FILE $BACKUP_FILE

echo "Patching $APP_FILE to ensure get_msisdn_location is called correctly..."

# Remove old call if present
sed -i '/get_msisdn_location(lookup_number)/d' $APP_FILE

# Insert correct logic near line 800 (or append if not found)
LINE_NUM=$(grep -n "lookup_number" $APP_FILE | head -1 | cut -d: -f1)
if [ -z "$LINE_NUM" ]; then
    LINE_NUM=800
fi

# Insert parse_logs and location call
sed -i "${LINE_NUM}i\\nfrom Tools.parse_radius_logs import parse_logs\nradius_logs = parse_logs()\nlocation = get_msisdn_location(lookup_number, radius_logs)\n" $APP_FILE

# Ensure location is passed to render_template
sed -i '/render_template/s/)/, location=location)/' $APP_FILE

# Format with black
if command -v black &> /dev/null; then
    echo "Formatting with black..."
    black $APP_FILE
else
    echo "black not found. Please run: pip install black"
fi

echo "Patch complete. You can now rerun the app and test live tower lookup."
