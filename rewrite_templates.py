import re
import shutil
from pathlib import Path

app_path = Path("app.py")
backup_path = app_path.with_suffix(".bak")

# Backup
shutil.copyfile(app_path, backup_path)
print("🔒 Backup created: app.py.bak")

template_map = {
    "dashboard.html": "admin/dashboard.html",
    "login.html": "auth/login.html",
    "lookup.html": "public/lookup.html",
    "viewer_dashboard.html": "admin/partials/viewer_dashboard.html",
    "admin_dashboard.html": "admin/partials/admin_dashboard.html",
    "base.html": "layouts/base.html",
    "admin_layout.html": "layouts/admin.html",
    "public_layout.html": "layouts/public.html",
    "forgot_password.html": "auth/forgot_password.html",
}

with open(app_path, "r", encoding="utf-8") as f:
    code = f.read()

# Replace template paths safely
for old, new in template_map.items():
    pattern = rf'render_template\(\s*[\'"]{old}[\'"]'
    code = re.sub(pattern, f'render_template("{new}"', code)

# Write back
with open(app_path, "w", encoding="utf-8") as f:
    f.write(code)

# Validate remaining unmapped templates
unresolved = re.findall(r'render_template\(\s*[\'"]([^\'"]+\.html)[\'"]', code)
unmapped = sorted(set([t for t in unresolved if t not in template_map.values()]))

if unmapped:
    print("⚠️ Unresolved template references:")
    for t in unmapped:
        print(f" - {t}")
else:
    print("✅ All render_template paths updated and verified.")
