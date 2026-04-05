#!/bin/bash
set -euo pipefail

echo "=== Gunicorn Project Auto-Installer ==="

CURRENT_USER="$(whoami)"
DEFAULT_OWNER_USER="$CURRENT_USER"
DEFAULT_OWNER_GROUP="$CURRENT_USER"
DEFAULT_SERVICE_USER="flaskuser"
DEFAULT_SERVICE_GROUP="flaskuser"
DEFAULT_DOMAIN_SUFFIX="snapmerc.com"
PYTHON="/usr/bin/python3"

read -r -p "Project name (no spaces): " PROJECT
read -r -p "Git SSH URL [git@github-daxarinn:daxarinn/${PROJECT}.git]: " GIT_URL
read -r -p "Domain [$PROJECT.$DEFAULT_DOMAIN_SUFFIX]: " DOMAIN
read -r -p "Code owner user [$DEFAULT_OWNER_USER]: " OWNER_USER
read -r -p "Code owner group [$DEFAULT_OWNER_GROUP]: " OWNER_GROUP
read -r -p "Service user [$DEFAULT_SERVICE_USER]: " SERVICE_USER
read -r -p "Service group [$DEFAULT_SERVICE_GROUP]: " SERVICE_GROUP
read -r -p "Daily scrape time [04:30]: " SCRAPE_TIME

GIT_URL=${GIT_URL:-git@github-daxarinn:daxarinn/${PROJECT}.git}
DOMAIN=${DOMAIN:-$PROJECT.$DEFAULT_DOMAIN_SUFFIX}
OWNER_USER=${OWNER_USER:-$DEFAULT_OWNER_USER}
OWNER_GROUP=${OWNER_GROUP:-$DEFAULT_OWNER_GROUP}
SERVICE_USER=${SERVICE_USER:-$DEFAULT_SERVICE_USER}
SERVICE_GROUP=${SERVICE_GROUP:-$DEFAULT_SERVICE_GROUP}
SCRAPE_TIME=${SCRAPE_TIME:-04:30}

BASE_DIR="/var/www/$PROJECT"
DATA_DIR="/var/www/database/$PROJECT"
BACKUP_DIR="/var/www/backups/$PROJECT"
ENV_FILE="$BASE_DIR/deploy/aa.env"
WEB_SERVICE="/etc/systemd/system/${PROJECT}-web.service"
SCRAPE_SERVICE="/etc/systemd/system/${PROJECT}-scrape.service"
SCRAPE_TIMER="/etc/systemd/system/${PROJECT}-scrape.timer"
NGINX_FILE="/etc/nginx/sites-available/$PROJECT"
UPDATE_SCRIPT="/home/$OWNER_USER/${PROJECT}-update.sh"

if ! id "$OWNER_USER" >/dev/null 2>&1; then
    echo "Owner user does not exist: $OWNER_USER"
    exit 1
fi

if ! getent group "$OWNER_GROUP" >/dev/null 2>&1; then
    echo "Owner group does not exist: $OWNER_GROUP"
    exit 1
fi

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
    echo "Service user does not exist: $SERVICE_USER"
    exit 1
fi

if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
    echo "Service group does not exist: $SERVICE_GROUP"
    exit 1
fi

PORT=$(comm -23 \
    <(seq 8000 8999 | sort) \
    <(sudo ss -tuln | awk '{print $5}' | grep -oP ':(\d+)$' | tr -d ':' | sort -u) \
    | head -n 1)

if [ -z "$PORT" ]; then
    echo "No free port available (8000-8999)."
    exit 1
fi

echo "Selected free port: $PORT"

echo "=== Ensure directories ==="
sudo mkdir -p "$BASE_DIR" "$DATA_DIR" "$BACKUP_DIR"

if [ -d "$BASE_DIR/.git" ]; then
    echo "=== Reusing existing checkout in $BASE_DIR ==="
else
    if [ -n "$(find "$BASE_DIR" -mindepth 1 -maxdepth 1 2>/dev/null)" ]; then
        echo "Target directory exists and is not empty: $BASE_DIR"
        exit 1
    fi
    echo "=== Clone repo ==="
    sudo chown -R "$OWNER_USER:$OWNER_GROUP" "$BASE_DIR"
    sudo -u "$OWNER_USER" git clone "$GIT_URL" "$BASE_DIR"
fi

echo "=== Ensure service user can read code ==="
sudo usermod -aG "$OWNER_GROUP" "$SERVICE_USER" || true
sudo chown -R "$OWNER_USER:$OWNER_GROUP" "$BASE_DIR"
sudo find "$BASE_DIR" -type d -exec chmod 750 {} \;
sudo find "$BASE_DIR" -type f -exec chmod 640 {} \;
sudo chmod -R g+rX "$BASE_DIR"

echo "=== Create virtualenv ==="
cd "$BASE_DIR"
sudo -u "$OWNER_USER" $PYTHON -m venv .venv
sudo -u "$OWNER_USER" "$BASE_DIR/.venv/bin/pip" install --upgrade pip
sudo -u "$OWNER_USER" "$BASE_DIR/.venv/bin/pip" install -r requirements.txt
sudo chmod -R g+rX "$BASE_DIR/.venv"

echo "=== Configure writable runtime directories ==="
sudo chown -R "$SERVICE_USER:$SERVICE_GROUP" "$DATA_DIR" "$BACKUP_DIR"
sudo chmod -R 750 "$DATA_DIR" "$BACKUP_DIR"

echo "=== Write environment file ==="
sudo mkdir -p "$BASE_DIR/deploy"
sudo bash -c "cat > '$ENV_FILE'" <<EOF
AA_DB_PATH=$DATA_DIR/meetings.sqlite
AA_CSV_PATH=$DATA_DIR/meetings_latest.csv
AA_BACKUP_DIR=$BACKUP_DIR
AA_BACKUP_KEEP_DAYS=30
EOF
sudo chown "$OWNER_USER:$SERVICE_GROUP" "$ENV_FILE"
sudo chmod 640 "$ENV_FILE"

echo "=== Create systemd web service ==="
sudo bash -c "cat > '$WEB_SERVICE'" <<EOF
[Unit]
Description=$PROJECT web service
After=network.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$BASE_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$BASE_DIR/.venv/bin/gunicorn --workers 3 --bind 127.0.0.1:$PORT wsgi:app
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

echo "=== Create systemd scrape service ==="
sudo bash -c "cat > '$SCRAPE_SERVICE'" <<EOF
[Unit]
Description=$PROJECT scrape and backup
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$BASE_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$BASE_DIR/.venv/bin/python $BASE_DIR/scripts/scrape_and_backup.py
StandardOutput=journal
StandardError=journal
EOF

echo "=== Create systemd scrape timer ==="
sudo bash -c "cat > '$SCRAPE_TIMER'" <<EOF
[Unit]
Description=Run $PROJECT scrape once per day

[Timer]
OnCalendar=*-*-* ${SCRAPE_TIME}:00
Persistent=true
Unit=${PROJECT}-scrape.service

[Install]
WantedBy=timers.target
EOF

echo "=== Run first scrape ==="
sudo -u "$SERVICE_USER" env \
    AA_DB_PATH="$DATA_DIR/meetings.sqlite" \
    AA_CSV_PATH="$DATA_DIR/meetings_latest.csv" \
    AA_BACKUP_DIR="$BACKUP_DIR" \
    AA_BACKUP_KEEP_DAYS="30" \
    "$BASE_DIR/.venv/bin/python" "$BASE_DIR/scripts/scrape_and_backup.py"

echo "=== Enable services ==="
sudo systemctl daemon-reload
sudo systemctl enable --now "${PROJECT}-web.service"
sudo systemctl enable --now "${PROJECT}-scrape.timer"

sleep 2
sudo systemctl status "${PROJECT}-web.service" --no-pager -n 20 || true
sudo systemctl status "${PROJECT}-scrape.timer" --no-pager -n 20 || true

echo "=== Create Nginx config ==="
sudo bash -c "cat > '$NGINX_FILE'" <<EOF
server {
    listen 80;
    server_name $DOMAIN;

    location / {
        proxy_pass http://127.0.0.1:$PORT;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

sudo ln -sf "$NGINX_FILE" "/etc/nginx/sites-enabled/$PROJECT"
sudo nginx -t
sudo systemctl reload nginx

echo "=== Request SSL certificate ==="
sudo certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos -m "admin@$DOMAIN" || true

echo "=== Create update script ==="
cat > "$UPDATE_SCRIPT" <<EOF
#!/bin/bash
set -euo pipefail
cd $BASE_DIR
git pull --ff-only
$BASE_DIR/.venv/bin/pip install -r requirements.txt
sudo systemctl restart ${PROJECT}-web.service
sudo systemctl start ${PROJECT}-scrape.service
EOF
chmod +x "$UPDATE_SCRIPT"

echo
echo "================================================="
echo "Project $PROJECT deployed successfully"
echo "Domain: https://$DOMAIN"
echo "Port: $PORT"
echo "Web logs: journalctl -u ${PROJECT}-web.service -f"
echo "Scrape logs: journalctl -u ${PROJECT}-scrape.service -f"
echo "Update with: $UPDATE_SCRIPT"
