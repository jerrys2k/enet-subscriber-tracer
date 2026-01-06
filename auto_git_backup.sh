#!/bin/bash

# === ✅ CONFIGURATION ===
PROJECT_DIR="/home/enet/msisdn_checker"
BRANCH="main"
LOG_FILE="$PROJECT_DIR/logs/git_backup.log"
TIMESTAMP=$(date +"%Y%m%d-%H%M")
TAG="snapshot-$TIMESTAMP"

cd "$PROJECT_DIR" || { echo "❌ Cannot cd into $PROJECT_DIR"; exit 1; }
mkdir -p "$PROJECT_DIR/logs"

echo "📦 [$TIMESTAMP] Starting Git backup..." >> "$LOG_FILE"

# === 🧠 COMMIT CHANGES IF ANY ===
if git diff-index --quiet HEAD --; then
  echo "ℹ️ No uncommitted changes." >> "$LOG_FILE"
else
  git add . >> "$LOG_FILE" 2>&1
  git commit -m "🔄 Auto Snapshot: $TIMESTAMP" >> "$LOG_FILE" 2>&1
  echo "✅ Committed local changes." >> "$LOG_FILE"
fi

# === 🏷️ CREATE TAG IF NOT EXISTS ===
if git tag | grep -q "$TAG"; then
  echo "⚠️ Tag $TAG already exists. Skipping tag creation." >> "$LOG_FILE"
else
  git tag "$TAG" >> "$LOG_FILE" 2>&1
  echo "🏷️ Created new tag: $TAG" >> "$LOG_FILE"
fi

# === 🚀 PUSH TO REMOTE ===
git push origin "$BRANCH" >> "$LOG_FILE" 2>&1
git push origin "$TAG" >> "$LOG_FILE" 2>&1
echo "🚀 Pushed branch + tag to GitHub" >> "$LOG_FILE"

# === 💤 OPTIONAL: REMOVE DESKTOP NOTIFY FOR HEADLESS ===
# Desktop notifications skipped (notify-send disabled)

# === 🧹 OPTIONAL: CLEANUP OLD TAGS (DISABLED) ===
# find .git/refs/tags -type f -mtime +7 -exec rm {} \;

echo "✅ Git backup completed at $TIMESTAMP" >> "$LOG_FILE"
echo "-------------------------------" >> "$LOG_FILE"
