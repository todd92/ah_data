#!/usr/bin/env bash
set -euo pipefail

# Navigate to the repository directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 1. Load environment variables from .env file if it exists
if [ -f .env ]; then
  # Load env vars ignoring comment/empty lines, resolving variables and quotes correctly
  eval "$(python3 -c '
import os, re, shlex
if os.path.exists(".env"):
    for line in open(".env"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$", line)
        if m:
            name, val = m.groups()
            if (val.startswith("\x27") and val.endswith("\x27")) or (val.startswith("\"") and val.endswith("\"")):
                val = val[1:-1]
            print(f"export {name}={shlex.quote(val)}")
')"
fi

# 2. Set default environment variables (matching GitHub Actions config)
export ARCHIVE_BRANCH="${ARCHIVE_BRANCH:-sqlite-history}"
export DB_PATH="${DB_PATH:-data/ah_prices.sqlite3}"
export REPORT_PATH="${REPORT_PATH:-report.json}"
export CONFIG_PATH="${CONFIG_PATH:-config.json}"
export WOW_REGION="${WOW_REGION:-us}"
export WOW_LOCALE="${WOW_LOCALE:-en_US}"
export WOW_DEFAULT_REALM_SLUG="${WOW_DEFAULT_REALM_SLUG:-dawnbringer}"
export WOW_TARGETS_FILE="${WOW_TARGETS_FILE:-targets_midnight_tailoring_enchanting.json}"
export WOW_EXPANSION_KEYWORD="${WOW_EXPANSION_KEYWORD:-midnight}"
export WOW_PROFESSIONS="${WOW_PROFESSIONS:-tailoring,enchanting,inscription,leatherworking,alchemy,blacksmithing,engineering,jewelcrafting,cooking,herbalism,mining,skinning,fishing}"
export WOW_WEBHOOK_PROFESSIONS="${WOW_WEBHOOK_PROFESSIONS:-tailoring,enchanting}"
export WOW_WEBHOOK_MIN_CRAFT_CONFIDENCE="${WOW_WEBHOOK_MIN_CRAFT_CONFIDENCE:-90}"
export WATCHLIST_DEBUG_DIR="${WATCHLIST_DEBUG_DIR:-watchlist_debug}"

# Try to load Blizzard credentials from config.json if not set in environment
if [ -z "${BLIZZARD_CLIENT_ID:-}" ] || [ -z "${BLIZZARD_CLIENT_SECRET:-}" ]; then
  if [ -f config.json ]; then
    echo "Blizzard credentials not found in environment, reading from config.json..."
    BLIZZARD_CLIENT_ID=$(python3 -c "import json; print(json.load(open('config.json')).get('client_id', ''))")
    BLIZZARD_CLIENT_SECRET=$(python3 -c "import json; print(json.load(open('config.json')).get('client_secret', ''))")
    export BLIZZARD_CLIENT_ID
    export BLIZZARD_CLIENT_SECRET
  fi
fi

# Ensure required credentials are set
if [ -z "${BLIZZARD_CLIENT_ID:-}" ] || [ -z "${BLIZZARD_CLIENT_SECRET:-}" ]; then
  echo "Error: BLIZZARD_CLIENT_ID and BLIZZARD_CLIENT_SECRET must be configured (in .env or config.json)" >&2
  exit 1
fi

# 3. Activate Python virtual environment
if [ -d .venv ]; then
  source .venv/bin/activate
elif [ -d venv ]; then
  source venv/bin/activate
else
  echo "Error: Virtual environment (.venv or venv) not found. Run 'make setup' or create it first." >&2
  exit 1
fi

# 4. Generate runtime config.json dynamically
python3 - <<'PY'
import json
import os

config = {
    "region": os.environ["WOW_REGION"],
    "locale": os.environ["WOW_LOCALE"],
    "default_realm_slug": os.environ["WOW_DEFAULT_REALM_SLUG"],
    "client_id": os.environ["BLIZZARD_CLIENT_ID"],
    "client_secret": os.environ["BLIZZARD_CLIENT_SECRET"],
    "targets_files": [
        os.environ["WOW_TARGETS_FILE"],
        "expansion_world_items.json"
    ],
}

with open(os.environ["CONFIG_PATH"], "w", encoding="utf-8") as fh:
    json.dump(config, fh, indent=2)
PY

# 5. Check if watchlist files exist; if not, rebuild them
if [ ! -f "expansion_world_items.json" ] || [ ! -f "$WOW_TARGETS_FILE" ]; then
  echo "Watchlist files missing. Rebuilding..."
  make watchlist UV=
fi

# 6. Run the scraper and monitor tool
rm -rf "$WATCHLIST_DEBUG_DIR"
python3 ah_monitor.py \
  --config "$CONFIG_PATH" \
  --report "$REPORT_PATH" \
  --db "$DB_PATH" \
  --metric weighted_avg_unit_price \
  --signal-direction both \
  --window-hours 168 \
  --sigma 2.0 \
  --min-history 24 \
  --trend-hours 48 \
  --min-trend-history 6 \
  --min-listings-commodity 8 \
  --min-quantity-commodity 200 \
  --min-listings-crafted 2 \
  --min-quantity-crafted 1 \
  --min-abs-move-gold-commodity 20 \
  --min-abs-move-gold-crafted 100 \
  --enable-predictions \
  --prediction-window-hours 168 \
  --prediction-short-window-hours 12 \
  --prediction-medium-window-hours 48 \
  --prediction-min-history 24 \
  --prediction-min-confidence 0.80 \
  --prediction-cooldown-hours 24 \
  --prediction-cooldown-horizon-hours 12 \
  --prediction-cooldown-grace-hours 6 \
  --prediction-cooldown-min-confidence 0.85 \
  --prediction-cooldown-loss-pct 20 \
  --retention-days-observations 0 \
  --retention-days-alerts 0 \
  --retention-days-predictions 0 \
  --enable-craft-alerts \
  --craft-ah-cut-rate 0.05 \
  --craft-min-profit-gold 50 \
  --craft-min-margin-pct 0.10 \
  --webhook-professions "$WOW_WEBHOOK_PROFESSIONS" \
  --webhook-min-craft-confidence "$WOW_WEBHOOK_MIN_CRAFT_CONFIDENCE" \
  --webhook-url "${AH_ALERT_WEBHOOK_URL:-}" \
  --webhook-format discord

# 7. Sync SQLite database to Google Drive (if rclone is configured)
# Avoid syncing if we are purely writing to PostgreSQL
DB_URL="${DATABASE_URL:-}"
if [[ ! "$DB_URL" =~ ^postgres ]]; then
  if [ -n "${RCLONE_CONFIG_DATA:-}" ]; then
    mkdir -p ~/.config/rclone
    echo "$RCLONE_CONFIG_DATA" > ~/.config/rclone/rclone.conf
  fi
  
  if command -v rclone &> /dev/null && [ -f ~/.config/rclone/rclone.conf ]; then
    make sync-gdrive
  else
    echo "rclone configuration not found or rclone not installed. Skipping Google Drive sync."
  fi
fi

echo "Scraper and monitor run completed successfully."
