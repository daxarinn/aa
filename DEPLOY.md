# Deploy

## 1. Git identity fyrir þetta repo

Keyrðu þetta inni í repo-inu:

```powershell
git config user.name "Nýtt nafn"
git config user.email "nytt@netfang.is"
```

Staðfestu:

```powershell
git config --get user.name
git config --get user.email
```

## 2. Sér SSH lykill fyrir þetta repo

Búðu til lykil:

```powershell
ssh-keygen -t ed25519 -C "aa-repo" -f $HOME/.ssh/id_ed25519_aa
```

Ræstu ssh-agent og bættu lyklinum við:

```powershell
Get-Service ssh-agent | Set-Service -StartupType Automatic
Start-Service ssh-agent
ssh-add $HOME/.ssh/id_ed25519_aa
```

Birtu public key:

```powershell
Get-Content $HOME/.ssh/id_ed25519_aa.pub
```

Settu hana inn hjá Git hosting þjónustunni.

## 3. SSH config fyrir annað login

Bættu þessu í `~/.ssh/config`:

```sshconfig
Host github-aa
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_ed25519_aa
    IdentitiesOnly yes
```

Prófaðu:

```powershell
ssh -T git@github-aa
```

## 4. Tengja remote og push-a

```powershell
git init
git add .
git commit -m "Initial commit"
git remote add origin git@github-aa:USERNAME_OR_ORG/aa.git
git branch -M main
git push -u origin main
```

## 5. VPS undirbúningur

Forsendur:

- app fer í `/var/www/aa`
- backups fara í `/var/www/backups/aa`
- nginx er nú þegar uppsett

Sem `root` eða með `sudo`:

```bash
mkdir -p /var/www/aa
mkdir -p /var/www/database/aa
mkdir -p /var/www/backups/aa
```

## 6. Clone og Python environment á VPS

Sem `dax` notandi:

```bash
cd /var/www
git clone git@github-aa:USERNAME_OR_ORG/aa.git aa
cd aa
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
cp deploy/aa.env.example deploy/aa.env
```

Keyrðu fyrsta scrape:

```bash
.venv/bin/python scripts/scrape_and_backup.py
```

## 7. systemd services

Afritaðu unit files:

```bash
sudo cp deploy/systemd/aa-web.service /etc/systemd/system/
sudo cp deploy/systemd/aa-scrape.service /etc/systemd/system/
sudo cp deploy/systemd/aa-scrape.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now aa-web.service
sudo systemctl enable --now aa-scrape.timer
```

Skoða status:

```bash
systemctl status aa-web.service
systemctl status aa-scrape.timer
```

Logs:

```bash
journalctl -u aa-web.service -f
journalctl -u aa-scrape.service -f
```

## 8. nginx reverse proxy

Afritaðu `deploy/nginx/aa.conf.example` yfir í þitt nginx setup og lagaðu:

- `server_name`
- mögulega TLS/certbot

Síðan:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## 9. Update flow

Þegar ný útgáfa fer á server:

```bash
cd /var/www/aa
git pull
. .venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart aa-web.service
sudo systemctl start aa-scrape.service
```

## 10. WSGI entrypoint og installer script

Flask appið er keyrt í production með:

- `wsgi.py`
- `gunicorn`

Ef þú vilt nota sjálfvirka VPS uppsetningu, notaðu:

```bash
deploy/setup_gunicorn_project.sh
```
