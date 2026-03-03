from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from backend_blockid.database.connection import DB_PATH, get_connection
from backend_blockid.tools import backup_db, run_full_pipeline, run_migrations

BACKUPS_DIR = Path("backend_blockid/database/backups")


def _git_commit_hash() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(Path.cwd()))
        return out.decode("utf-8").strip()
    except Exception:
        return "unknown"


def _validate_schema(required: list[str]) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    ok = True
    for table in required:
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        if cur.fetchone() is None:
            print("Missing table:", table)
            ok = False
    conn.close()
    return ok


def _latest_backup() -> Path | None:
    if not BACKUPS_DIR.exists():
        return None
    backups = sorted(BACKUPS_DIR.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    return backups[0] if backups else None


def _restore_backup(backup_path: Path) -> None:
    if not backup_path.exists():
        print("Backup file not found for restore:", backup_path)
        return
    shutil.copy2(backup_path, DB_PATH)
    print("Restored backup:", backup_path)


def _ensure_deploy_log() -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS deploy_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deploy_time INTEGER,
            git_commit_hash TEXT,
            db_backup_file TEXT,
            result TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def _log_deploy(backup_file: str, result: str) -> None:
    _ensure_deploy_log()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO deploy_log(deploy_time, git_commit_hash, db_backup_file, result)
        VALUES (?, ?, ?, ?)
        """,
        (int(time.time()), _git_commit_hash(), backup_file, result),
    )
    conn.commit()
    conn.close()


def deploy() -> None:
    print("\n=== BLOCKID DEPLOY START ===\n")

    try:
        backup_path = backup_db.create_backup()
        if backup_path is None:
            print("Backup snapshot not created. Aborting.")
            _log_deploy("", "backup_failed")
            sys.exit(1)
        print("Backup snapshot created")
    except Exception as e:
        print("Backup failed:", e)
        _log_deploy("", "backup_failed")
        sys.exit(1)

    required_tables = ["trust_scores", "wallet_reasons", "transactions", "priority_wallets"]
    if not _validate_schema(required_tables):
        print("Schema validation failed")
        _log_deploy(str(backup_path), "schema_failed")
        sys.exit(1)
    print("Schema validated")

    confirm = input("Apply migrations? yes/no: ").strip().lower()
    if confirm != "yes":
        print("Deploy cancelled.")
        _log_deploy(str(backup_path), "cancelled")
        return

    try:
        run_migrations.run()
        print("Migrations applied")
    except Exception as e:
        print("Migration failed:", e)
        _restore_backup(_latest_backup() or backup_path)
        _log_deploy(str(backup_path), "migration_failed")
        sys.exit(1)

    try:
        os.environ["BLOCKID_TEST_MODE"] = "1"
        run_full_pipeline.main()
        print("Dry-run pipeline success")
    except Exception as e:
        print("Dry-run pipeline failed:", e)
        _restore_backup(_latest_backup() or backup_path)
        _log_deploy(str(backup_path), "dry_run_failed")
        sys.exit(1)
    finally:
        if "BLOCKID_TEST_MODE" in os.environ:
            os.environ.pop("BLOCKID_TEST_MODE")

    try:
        print("Running production pipeline...")
        run_full_pipeline.main()
        print("Production pipeline success")
        _log_deploy(str(backup_path), "success")
    except Exception as e:
        print("Pipeline failed:", e)
        _restore_backup(_latest_backup() or backup_path)
        _log_deploy(str(backup_path), "pipeline_failed")
        sys.exit(1)

    print("\n=== DEPLOY COMPLETE ===\n")


if __name__ == "__main__":
    deploy()
