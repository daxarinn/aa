from __future__ import annotations

import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main import scrape_all


def backup_file(path: Path, backup_dir: Path, stamp: str) -> None:
    if not path.exists():
        return
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / f"{path.stem}-{stamp}{path.suffix}"
    shutil.copy2(path, target)


def prune_old_backups(backup_dir: Path, keep_days: int) -> None:
    if not backup_dir.exists():
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)
    for item in backup_dir.iterdir():
        if not item.is_file():
            continue
        modified = datetime.fromtimestamp(item.stat().st_mtime, tz=timezone.utc)
        if modified < cutoff:
            item.unlink()


def main() -> int:
    db_path = Path(os.environ.get("AA_DB_PATH", "data/meetings.sqlite"))
    csv_path = Path(os.environ.get("AA_CSV_PATH", "exports/meetings_latest.csv"))
    backup_dir = Path(os.environ.get("AA_BACKUP_DIR", "data/backups"))
    keep_days = int(os.environ.get("AA_BACKUP_KEEP_DAYS", "30"))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    scrape_all(db_path, csv_path, copy_to_clipboard=False)
    backup_file(db_path, backup_dir, stamp)
    backup_file(csv_path, backup_dir, stamp)
    prune_old_backups(backup_dir, keep_days)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
