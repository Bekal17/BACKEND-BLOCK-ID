from __future__ import annotations

import os
import sys


def main() -> int:
    env = os.getenv("BLOCKID_ENV", "DEV").upper()
    skip_backup = os.getenv("BLOCKID_SKIP_BACKUP", "0") == "1"
    dry_run = os.getenv("BLOCKID_DRY_RUN", "0") == "1"

    print(f"\n=== BLOCKID RUNNER MODE: {env} ===\n")

    if not skip_backup:
        try:
            from backend_blockid.tools import backup_db
            print("Creating DB backup...")
            backup_db.create_backup()
        except Exception as e:
            print("Backup failed:", e)
            return 1
    else:
        print("Skipping DB backup (BLOCKID_SKIP_BACKUP=1)")

    if env == "DEV":
        try:
            from backend_blockid.tools import run_migrations
            print("Running migrations...")
            run_migrations.run()
        except Exception as e:
            print("Migration failed:", e)
            return 1

        try:
            from backend_blockid.tools import seed_priority_wallets
            print("Seeding priority wallets...")
            seed_priority_wallets.main()
        except Exception as e:
            print("Seed failed:", e)
            return 1
    else:
        print("PRODUCTION MODE: skipping migrations and seed.")

    if dry_run:
        print("Dry run enabled (BLOCKID_DRY_RUN=1). Skipping pipeline.")
        print("\n=== BLOCKID RUN COMPLETE ===\n")
        return 0

    try:
        from backend_blockid.tools.verify_schema import verify_schema
        print("Verifying DB schema...")
        verify_schema()
    except Exception as e:
        print("Schema verification failed:", e)
        return 1

    try:
        from backend_blockid.tools.health_check import run_health_check
        print("Running health check...")
        run_health_check()
    except Exception as e:
        print("Health check failed:", e)
        return 1

    try:
        from backend_blockid.tools import run_full_pipeline
        print("Running full pipeline...")
        run_full_pipeline.main()
    except Exception as e:
        print("Pipeline failed:", e)
        return 1

    print("\n=== BLOCKID RUN COMPLETE ===\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
