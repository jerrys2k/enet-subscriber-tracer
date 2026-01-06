#!/bin/bash

# Define paths
TEMPLATE_PATH="templates/admin/dashboard.html"
BACKUP_PATH="${TEMPLATE_PATH}.bak"

# Check that file exists
if [[ ! -f "$TEMPLATE_PATH" ]]; then
    echo "❌ File not found: $TEMPLATE_PATH"
    exit 1
fi

# Make a backup
cp "$TEMPLATE_PATH" "$BACKUP_PATH"
echo "📦 Backup created at: $BACKUP_PATH"

# Inject the location panel (if not already present)
if grep -q "Last Known Tower Location" "$TEMPLATE_PATH"; then
    echo "✅ Location panel already present. No changes made."
    exit 0
fi

# Append the Tailwind location block before the endblock
sed -i '/{% endblock %}/i \
{% if location %}\
<div class="mt-6 p-5 border border-gray-200 rounded-2xl shadow bg-white">\
  <h2 class="text-xl font-bold text-gray-700 mb-4 flex items-center gap-2">\
    <span>📍 Last Known Tower Location</span>\
    <span class="text-xs bg-green-100 text-green-800 px-2 py-0.5 rounded">source: {{ location.source }}</span>\
  </h2>\
  <dl class="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-2 text-sm">\
    <div><dt class="font-semibold">Tower</dt><dd>{{ location.tower_name }}</dd></div>\
    <div><dt class="font-semibold">Cell ID</dt><dd>{{ location.cell_id }}</dd></div>\
    <div><dt class="font-semibold">eNodeB</dt><dd>{{ location.enodeb_id }}</dd></div>\
    <div><dt class="font-semibold">Sector</dt><dd>{{ location.sector }}</dd></div>\
    <div><dt class="font-semibold">Cluster</dt><dd>{{ location.cluster }}</dd></div>\
    <div><dt class="font-semibold">GPS</dt><dd><a href="https://earth.google.com/web/search/{{ location.lat }},{{ location.lon }}" target="_blank" class="text-blue-600 underline">{{ location.lat }}, {{ location.lon }}</a></dd></div>\
    <div><dt class="font-semibold">Timestamp</dt><dd>{{ location.timestamp }}</dd></div>\
  </dl>\
</div>\
{% endif %}\
' "$TEMPLATE_PATH"

echo "✅ Tower location panel injected into: $TEMPLATE_PATH"
