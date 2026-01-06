import re
import shutil

# Define mappings
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

# Path to app.py
APP_PATH = "app.py"
BACKUP_PATH = "app.py.bak"

# Create backup
shutil.copyfile(APP_PATH, BACKUP_PATH)
print("🔒 Backup created at app.py.bak")

with open(APP_PATH, "r") as f:
    code = f.read()

# Replace all render_template calls
for old, new in template_map.items():
    pattern = rf'render_template\(\s*[\'"]{old}[\'"]'
    replacement = f'render_template("{new}"'
    code = re.sub(pattern, replacement, code)

with open(APP_PATH, "w") as f:
    f.write(code)

print("✅ Template paths updated in app.py.")
