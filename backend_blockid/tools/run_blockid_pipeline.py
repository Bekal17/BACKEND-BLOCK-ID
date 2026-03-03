from __future__ import annotations

import os
import sys


def _should_skip(name: str) -> bool:
    return (os.getenv(name) or "").strip() == "1"


def main() -> None:
    print("\n=== BLOCKID PIPELINE START ===\n")

    try:
        from backend_blockid.tools import backup_db
        print("Creating DB backup...")
        backup_db.create_backup()
    except Exception as e:
        print("Backup failed:", e)
        sys.exit(1)

    if not _should_skip("BLOCKID_SKIP_MIGRATIONS"):
        try:
            from backend_blockid.tools import run_migrations
            print("Running migrations...")
            run_migrations.run()
        except Exception as e:
            print("Migration failed:", e)
            sys.exit(1)

    if not _should_skip("BLOCKID_SKIP_SEED"):
        try:
            from backend_blockid.tools import seed_priority_wallets
            print("Seeding priority wallets...")
            seed_priority_wallets.main()
        except Exception as e:
            print("Seed failed:", e)
            sys.exit(1)

    try:
        from backend_blockid.tools import run_full_pipeline
        print("Running full pipeline...")
        run_full_pipeline.main()
    except Exception as e:
        print("Pipeline failed:", e)
        sys.exit(1)

    print("\n=== BLOCKID PIPELINE COMPLETE ===\n")


if __name__ == "__main__":
    main()
