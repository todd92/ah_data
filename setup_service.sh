#!/usr/bin/env bash
# Script to automate setting up the WoW AH Discord Bot as a systemd service on the Oracle VM.
# Run this on your VM: sudo ./setup_service.sh

# Exit on error
set -e

SERVICE_FILE="/etc/systemd/system/wow-ah-bot.service"
APP_DIR="/home/opc/ah_data"

echo "⚙️ Setting up WoW AH Discord Bot as a persistent systemd service..."

# 1. Verify we are running on the VM and the directory exists
if [ ! -d "$APP_DIR" ]; then
    echo "❌ Error: App directory $APP_DIR does not exist. Make sure you are running this on your Oracle VM."
    exit 1
fi

# 2. Check if we have sudo privileges
if [ "$EUID" -ne 0 ]; then
    echo "⚠️ Please run this script with sudo (e.g., sudo ./setup_service.sh)"
    exit 1
fi

# 3. Handle environment file location (Oracle Linux SELinux compatibility)
# Systemd under SELinux is blocked from reading files in /home/opc/.
# We copy the .env file to /etc/default/wow-ah-bot which is fully permitted.
ENV_DEST="/etc/default/wow-ah-bot"
echo "🔑 Copying environment file to $ENV_DEST..."
cp "$APP_DIR/.env" "$ENV_DEST"
chmod 600 "$ENV_DEST"
chown opc:opc "$ENV_DEST"

# 4. Detect Python executable path dynamically
# Systemd needs the absolute path to the Python executable running in the virtual environment.
PYTHON_PATH=""

# Check standard virtualenv folders first
if [ -f "$APP_DIR/.venv/bin/python" ]; then
    PYTHON_PATH="$APP_DIR/.venv/bin/python"
elif [ -f "$APP_DIR/venv/bin/python" ]; then
    PYTHON_PATH="$APP_DIR/venv/bin/python"
fi

# Check if uv is in opc's home directories to locate python
if [ -z "$PYTHON_PATH" ]; then
    UV_PATHS=(
        "/home/opc/.local/bin/uv"
        "/home/opc/.cargo/bin/uv"
        "/usr/bin/uv"
        "/usr/local/bin/uv"
    )
    for uv_bin in "${UV_PATHS[@]}"; do
        if [ -f "$uv_bin" ]; then
            # Run uv as 'opc' inside app dir to find the python path
            PYTHON_PATH=$(cd "$APP_DIR" && sudo -u opc "$uv_bin" run which python 2>/dev/null || echo "")
            if [ -n "$PYTHON_PATH" ]; then
                break
            fi
        fi
    done
fi

# Fallback to system python3/python
if [ -z "$PYTHON_PATH" ]; then
    PYTHON_PATH=$(which python3 || which python || echo "")
fi

if [ -z "$PYTHON_PATH" ]; then
    echo "❌ Error: Could not locate Python executable or virtual environment."
    exit 1
fi

echo "🔎 Detected Python path: $PYTHON_PATH"

# 5. Create the systemd service configuration
echo "💾 Writing service file to $SERVICE_FILE..."
cat <<EOF > "$SERVICE_FILE"
[Unit]
Description=WoW Auction House Discord AI Bot
After=network.target

[Service]
Type=simple
User=opc
WorkingDirectory=$APP_DIR
ExecStart=$PYTHON_PATH $APP_DIR/ah_discord_bot.py
Restart=always
RestartSec=10
EnvironmentFile=$ENV_DEST

[Install]
WantedBy=multi-user.target
EOF

# 4. Reload systemd daemon to pick up the new service
echo "🔄 Reloading systemd daemon..."
systemctl daemon-reload

# 5. Enable the service to start automatically on boot
echo "🔌 Enabling wow-ah-bot service..."
systemctl enable wow-ah-bot.service

# 6. Start the service
echo "🚀 Starting wow-ah-bot service..."
systemctl start wow-ah-bot.service

# 7. Print the current status
echo "🔍 Checking service status..."
sleep 2
systemctl status wow-ah-bot.service || true

echo "=================================================="
echo "✅ Setup Complete!"
echo "• To view logs in real-time, run: journalctl -u wow-ah-bot.service -f"
echo "• To stop the bot, run: sudo systemctl stop wow-ah-bot"
echo "• To restart the bot, run: sudo systemctl restart wow-ah-bot"
echo "=================================================="
