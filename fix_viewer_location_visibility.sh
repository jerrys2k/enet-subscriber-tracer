#!/bin/bash

TEMPLATE="templates/admin/dashboard.html"
BACKUP="templates/admin/dashboard.html.bak"

# Verify target file
if [[ ! -f "$TEMPLATE" ]]; then
  echo "❌ Error: $TEMPLATE not found"
  exit 1
fi

# Backup original
cp "$TEMPLATE" "$BACKUP"
echo "📦 Backup created at $BACKUP"

# Extract the tower block (by keyword match range)
START_LINE=$(grep -n "<h3 class=\"font-semibold text-lg mb-2\"><i class=\"fas fa-map-marker-alt\"></i> 📍 Last Known Tower Location</h3>" "$TEMPLATE" | cut -d: -f1)
END_LINE=$(tail -n +"$START_LINE" "$TEMPLATE" | grep -n "{% endif %}" | head -1 | awk -v s=$START_LINE '{print s + $1 - 1}')

# Remove original block
sed -i "${START_LINE},${END_LINE}d" "$TEMPLATE"
echo "🧽 Removed tower block from inside lookup-only section"

# Insert tower block just above Porting History
sed -i '/<!-- 🔁 Porting History -->/i\
{% if location %}\
<div class="bg-gray-50 p-4 rounded shadow mt-6">\
  <h3 class="font-semibold text-lg mb-2"><i class="fas fa-map-marker-alt"></i> 📍 Last Known Tower Location</h3>\
  <p><strong>Tower:</strong> {{ location.tower_name }}</p>\
  <p><strong>eNodeB ID:</strong> {{ location.enodeb_id }}</p>\
  <p><strong>Cell ID:</strong> {{ location.cell_id }}</p>\
  <p><strong>Latitude:</strong> {{ location.lat }}</p>\
  <p><strong>Longitude:</strong> {{ location.lon }}</p>\
  <p><strong>Timestamp:</strong> {{ location.timestamp }}</p>\
  <p class="mt-2"><a href="https://earth.google.com/web/search/{{ location.lat }},{{ location.lon }}" target="_blank" class="text-blue-600 hover:underline font-semibold">🌍 View on Google Earth</a></p>\
  <p class="text-sm text-gray-500 italic mt-2">Source: {{ location.source }}</p>\
</div>\
{% endif %}\
' "$TEMPLATE"

echo "✅ Tower block repositioned for all roles (viewer/admin)"
