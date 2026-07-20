#!/usr/bin/env bash
# Preemptive VM Health Check & Discord Alert Script
# Runs every 15 minutes via cron

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 1. Load Discord Webhook URL from .env if available
if [ -f .env ]; then
    export $(grep -E '^[A-Z_]+=' .env | xargs)
fi

WEBHOOK_URL="${AH_ALERT_WEBHOOK_URL:-}"

# Function to send Discord notification
send_discord_alert() {
    local message="$1"
    if [ -n "$WEBHOOK_URL" ]; then
        curl -s -H "Content-Type: application/json" \
             -X POST \
             -d "{\"content\": \"$message\"}" \
             "$WEBHOOK_URL" > /dev/null
    fi
}

# 2. Check RAM (Available Memory in MB)
AVAILABLE_RAM=$(free -m | awk '/^Mem:/ {print $7}')
if [ "$AVAILABLE_RAM" -lt 50 ]; then
    ALERT_MSG="⚠️ **PREEMPTIVE VM ALERT**: Available RAM is low (${AVAILABLE_RAM}MB left). Freeing OS memory cache..."
    echo "$ALERT_MSG"
    send_discord_alert "$ALERT_MSG"
    # Auto-Self-Healing: Drop caches if memory is low
    sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null 2>&1
fi

# 3. Check Swap Usage Percentage
SWAP_TOTAL=$(free -m | awk '/^Swap:/ {print $2}')
SWAP_USED=$(free -m | awk '/^Swap:/ {print $3}')
if [ "$SWAP_TOTAL" -gt 0 ]; then
    SWAP_PCT=$(( SWAP_USED * 100 / SWAP_TOTAL ))
    if [ "$SWAP_PCT" -gt 80 ]; then
        ALERT_MSG="⚠️ **PREEMPTIVE VM ALERT**: High Swap Usage (${SWAP_PCT}% used: ${SWAP_USED}MB / ${SWAP_TOTAL}MB). VM may experience disk thrashing!"
        echo "$ALERT_MSG"
        send_discord_alert "$ALERT_MSG"
    fi
fi

# 4. Check Disk Usage Percentage
DISK_PCT=$(df / | awk 'NR==2 {print $5}' | tr -d '%')
if [ "$DISK_PCT" -gt 85 ]; then
    ALERT_MSG="⚠️ **PREEMPTIVE VM ALERT**: High Disk Usage (${DISK_PCT}% used on root partition)."
    echo "$ALERT_MSG"
    send_discord_alert "$ALERT_MSG"
fi

# 5. Check Stale Lockfile
LOCKFILE="/tmp/wow_ah_scraper.lock"
if [ -f "$LOCKFILE" ]; then
    # Check if lockfile is older than 2 hours (7200 seconds)
    FILE_AGE=$(($(date +%s) - $(stat -c %Y "$LOCKFILE")))
    if [ "$FILE_AGE" -gt 7200 ]; then
        ALERT_MSG="⚠️ **PREEMPTIVE VM ALERT**: Stale scraper lockfile found (age: ${FILE_AGE}s). Removing lockfile automatically."
        echo "$ALERT_MSG"
        send_discord_alert "$ALERT_MSG"
        rm -f "$LOCKFILE"
    fi
fi

echo "VM Health Check finished cleanly. RAM available: ${AVAILABLE_RAM}MB, Disk: ${DISK_PCT}%."
