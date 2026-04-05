#!/bin/bash
set -euo pipefail

echo "=== Gunicorn Project Auto-Installer ==="

read -r -p "Project name (no spaces): " PROJECT
read -r -p "Git SSH URL (git@github.com:...): " GIT_URL
read -r -p "Domain [$PROJECT.snapmerc.com]: " DOMAIN
read -r -p "Service user [flaskuser]: " SERVICE_USER
read -r -p "Service group [flaskuser]: " SERVICE_GROUP

DOMAIN=${DOMAIN:-$PROJECT.snapmerc.com}
SERVICE_USER=${SERVICE_USER:-flaskuser}
SERVICE_GROUP=${SERVICE_GROUP:-flaskuser}

BASE_DIR="/var/www/$PROJECT"
DATA_DIR="/var/www/database/$PROJECT"
BACKUP_DIR="/var/www/backups/$PROJECT"
PYTHON="/usr/bin/python3"
CURRENT_USER="$(whoami)"

if [ -d "$BASE_DIR" ]; then
    echo "Project already exists: $BASE_DIR"
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

echo "=== Clone repo ==="
sudo mkdir -p "$BASE_DIR"
sudo chown -R "$CURRENT_USER:$CURRENT_USER" "$BASE_DIR"
git clone "$GIT_URL" "$BASE_DIR"

echo "=== Create virtualenv ==="
cd "$BASE_DIR"
$PYTHON -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

echo "=== Configure app env ==="
sudo mkdir -p "$DATA_DIR" "$BACKUP_DIR"
sudo chown -R "$SERVICE_USER:$SERVICE_GROUP" "$DATA_DIR" "$BACKUP_DIR"

mkdir -p "$BASE_DIR/deploy"
cat > "$BASE_DIR/deploy/aa.env" <<EOF
AA_DB_PATH=$DATA_DIR/meetings.sqlite
AA_CSV_PATH=$DATA_DIR/meetings_latest.csv
AA_BACKUP_DIR=$BACKUP_DIR
AA_BACKUP_KEEP_DAYS=30
EOF

echo "=== Create systemd web service ==="
WEB_SERVICE="/etc/systemd/system/${PROJECT}-web.service"
sudo bash -c "cat > $WEB_SERVICE" <<EOF
[Unit]
Description=$PROJECT web service
After=network.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$BASE_DIR
EnvironmentFile=$BASE_DIR/deploy/aa.env
ExecStart=$BASE_DIR/.venv/bin/gunicorn --workers 3 --bind 127.0.0.1:$PORT wsgi:app
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

echo "=== Create systemd scrape service ==="
SCRAPE_SERVICE="/etc/systemd/system/${PROJECT}-scrape.service"
sudo bash -c "cat > $SCRAPE_SERVICE" <<EOF
[Unit]
Description=$PROJECT scrape and backup
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$BASE_DIR
EnvironmentFile=$BASE_DIR/deploy/aa.env
ExecStart=$BASE_DIR/.venv/bin/python scripts/scrape_and_backup.py
StandardOutput=journal
StandardError=journal
EOF

echo "=== Create systemd scrape timer ==="
SCRAPE_TIMER="/etc/systemd/system/${PROJECT}-scrape.timer"
sudo bash -c "cat > $SCRAPE_TIMER" <<EOF
[Unit]
Description=Run $PROJECT scrape once per day

[Timer]
OnCalendar=*-*-* 04:30:00
Persistent=true
Unit=${PROJECT}-scrape.service

[Install]
WantedBy=timers.target
EOF

echo "=== Enable services ==="
sudo systemctl daemon-reload
sudo systemctl enable --now "${PROJECT}-web.service"
sudo systemctl enable --now "${PROJECT}-scrape.timer"
sudo systemctl start "${PROJECT}-scrape.service"

sleep 2
sudo systemctl status "${PROJECT}-web.service" --no-pager -n 20 || true
sudo systemctl status "${PROJECT}-scrape.timer" --no-pager -n 20 || true

echo "=== Create Nginx config ==="
NGINX_FILE="/etc/nginx/sites-available/$PROJECT"
sudo bash -c "cat > $NGINX_FILE" <<EOF
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
UPDATE_SCRIPT="/home/$CURRENT_USER/${PROJECT}-update.sh"
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
