# Migrating WoW AH Scraper from GitHub Actions to Oracle Cloud VM

This guide provides a comprehensive walkthrough for deploying, scheduling, and running the WoW AH Scraper and Monitor on an Oracle Cloud Infrastructure (OCI) Always Free VM (Ubuntu 22.04 / 24.04 LTS recommended) instead of GitHub Actions.

---

## Why Move to an Oracle Cloud VM?

1. **Persistent Storage**: The SQLite database (`data/ah_prices.sqlite3`) stays locally on the VM's disk. You no longer need to commit and push database snapshots back to GitHub on every run.
2. **Simplified Secret Management**: Keep credentials secure in a single local `.env` file instead of managing them across GitHub Actions secrets and environment variables.
3. **No Execution Limits**: GitHub Actions has billing limits and execution timeouts. A VM runs 24/7 with zero limits.
4. **Flexible Backups**: You can sync to Google Drive via `rclone` or directly stream all updates to a PostgreSQL database (like Supabase).

---

## Step-by-Step Deployment Guide

### Step 1: Install System Dependencies on the VM

Connect to your Oracle VM via SSH and update the system. Install Git, Python 3.12 (or latest Python 3), virtualenv tool (`python3-venv`), and `rclone` (if you plan to sync to Google Drive):

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Install git, python3, pip, and venv
sudo apt install -y git python3 python3-pip python3-venv rclone
```

### Step 2: Clone the Repository

Clone your repository to a directory on the VM (e.g., in your user's home folder):

```bash
git clone https://github.com/YOUR_USERNAME/ah_data.git ~/ah_data
cd ~/ah_data
```

### Step 3: Set Up the Virtual Environment & Dependencies

Set up a local Python virtual environment to isolate dependencies. Use the existing `Makefile` command:

```bash
# Using Makefile
make setup

# Or manually:
# python3 -m venv .venv
# source .venv/bin/activate
# pip install -r requirements.txt
```

> [!NOTE]
> If you are syncing data to PostgreSQL (Supabase/Neon), `requirements.txt` contains `psycopg[binary]`. If you only use SQLite, `psycopg` is not strictly required but safe to keep installed.

---

### Step 4: Configure the `.env` File

Create a `.env` file in the root of the repository (`~/ah_data/.env`). This file will house all your configurations and API credentials.

```bash
nano .env
```

Paste the following template and replace it with your actual secrets and variables:

```ini
# Blizzard API Credentials
BLIZZARD_CLIENT_ID="your_blizzard_client_id"
BLIZZARD_CLIENT_SECRET="your_blizzard_client_secret"

# Discord Alert Webhook URL
AH_ALERT_WEBHOOK_URL="https://discord.com/api/webhooks/..."

# Database Configuration (Choose ONE)
# Option A: Supabase/PostgreSQL connection string (Recommended)
DATABASE_URL="postgresql://postgres:<password>@<host>:6543/postgres?pgbouncer=true"

# Option B: SQLite local file (Leave DATABASE_URL commented or empty)
# DATABASE_URL=

# (Optional) Google Drive Rclone Config
# Only required if using Option B (SQLite) and you want automated GDrive backups
RCLONE_CONFIG_DATA="[gdrive]\ntype = drive\nclient_id = ...\nclient_secret = ...\n..."
```

---

### Step 5: Configure Google Drive Backup (If using SQLite)

If you are using SQLite instead of PostgreSQL, you can back up the DB to Google Drive. The script uses `rclone` to copy the DB.

If you don't want to deal with `RCLONE_CONFIG_DATA` env var strings, you can configure it interactively once on the VM:

```bash
rclone config
```

Configure a remote named **`gdrive`** following the prompts to authenticate with your Google Drive account. Once configured, test it:

```bash
make sync-gdrive
```

---

### Step 6: Test Execution via the Runner Script

We created a custom executable script [run_oracle.sh](file:///home/toddglad/projects/personal/ah_data/run_oracle.sh) to handle the complete execution cycle.

Verify that the script runs correctly:

```bash
./run_oracle.sh
```

Ensure it fetches data, processes the statistics, issues alerts (if any), and synchronizes with your database/GDrive successfully.

---

### Step 7: Schedule Automations using Cron

To replicate the 30-minute interval from GitHub Actions, schedule the execution using the VM's built-in `cron` daemon:

1. Open the crontab editor:
   ```bash
   crontab -e
   ```
2. Add the following line at the bottom of the file (replace `/home/ubuntu/ah_data` with your exact repository path):
   ```cron
   */30 * * * * /home/ubuntu/ah_data/run_oracle.sh >> /home/ubuntu/ah_data/cron_run.log 2>&1
   ```
3. Save and close the editor.

This command runs [run_oracle.sh](file:///home/toddglad/projects/personal/ah_data/run_oracle.sh) every 30 minutes, writing all execution logs (stdout and stderr) to `cron_run.log` so you can debug any runtime issues.

---

## Monitoring and Maintenance

- **View Execution Logs**: Check the cron execution logs at any time to monitor script progress:
  ```bash
  tail -f ~/ah_data/cron_run.log
  ```
- **Updating the Scraper**: To fetch updates or code changes you push to GitHub, just run:
  ```bash
  git pull
  ```
  *(If you made changes to requirements, run `make setup` again).*
