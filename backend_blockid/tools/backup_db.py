from __future__ import annotations

import os
import shutil
import time
from pathlib import Path

from backend_blockid.database.connection import DB_PATH

BACKUPS_DIR = Path("backend_blockid/database/backups")
MAX_BACKUPS = 20


def _timestamp() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())


def cleanup_old_backups(keep_last: int = 20) -> None:
    if not BACKUPS_DIR.exists():
        return
    files = list(BACKUPS_DIR.glob("*.db"))
    if len(files) <= keep_last:
        return
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    old_files = files[keep_last:]
    for f in old_files:
        try:
            f.unlink()
            print("Deleted old backup:", f.name)
        except Exception as e:
            print("Failed to delete:", f.name, e)


def create_backup() -> Path | None:
    if (os.getenv("BLOCKID_TEST_MODE") or "").strip() == "1":
        print("[backup] skipped (BLOCKID_TEST_MODE=1)")
        return None

    db_path = Path(DB_PATH)
    if not db_path.exists():
        print("[backup] DB file not found:", db_path)
        return None

    if db_path.stat().st_size > 1_000_000_000:
        print("[backup] WARNING: DB > 1GB:", db_path.stat().st_size)

    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
    ts = _timestamp()
    backup_name = f"{db_path.stem}_{ts}{db_path.suffix}"
    backup_path = BACKUPS_DIR / backup_name
    shutil.copy2(db_path, backup_path)
    cleanup_old_backups(keep_last=MAX_BACKUPS)
    print("Backup created:", backup_path.name)
    return backup_path
