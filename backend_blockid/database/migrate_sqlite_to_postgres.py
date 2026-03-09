import asyncio
import sqlite3
import asyncpg
import os

from dotenv import load_dotenv

load_dotenv()

SQLITE_DB = "D:/BACKENDBLOCKID/blockid.db"

DATABASE_URL = os.getenv("DATABASE_URL")


# URUTAN MIGRASI (sudah aman FK)
TABLES = [

    # base
    "tracked_wallets",
    "wallet_profiles",
    "wallet_meta",
    "wallet_last_update",
    "wallet_scan_meta",
    "scam_wallets",

    # transactions
    "transactions",
    "wallet_graph_edges",

    # clusters
    "wallet_clusters",
    "wallet_cluster_members",

    # entity
    "entity_profiles",
    "entity_reputation_history",

    # scoring
    "wallet_scores",
    "wallet_reasons",
    "wallet_reason_evidence",
    "wallet_history",
    "wallet_risk_probabilities",
    "trust_scores",

    # monitoring
    "alerts",
    "wallet_rolling_stats",
    "wallet_escalation_state",
    "wallet_reputation_state",

    # metadata
    "wallet_badges",
    "wallet_priority",
    "priority_wallets",

    # logs
    "helius_usage",
    "pipeline_run_log",
    "review_queue",
    "blockid_logs",

    # history
    "score_history"
]


def fix_value(v):
    """Convert SQLite values to PostgreSQL safe values"""

    if v == "":
        return None

    if isinstance(v, int) and v in (0, 1):
        return bool(v)

    return v


async def migrate():

    print("Connecting SQLite...")
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_conn.row_factory = sqlite3.Row
    cur = sqlite_conn.cursor()

    print("Connecting PostgreSQL...")
    pg = await asyncpg.connect(DATABASE_URL)

    for table in TABLES:

        print(f"\nMigrating {table}...")

        try:

            rows = cur.execute(f"SELECT * FROM {table}").fetchall()

        except Exception:

            print("  table not found, skipping")
            continue

        if not rows:

            print("  no rows")
            continue

        columns = rows[0].keys()

        col_list = ",".join(columns)

        placeholders = ",".join(
            [f"${i+1}" for i in range(len(columns))]
        )

        query = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
        """

        copied = 0
        skipped = 0

        for row in rows:

            fixed_row = [fix_value(v) for v in row]

            try:

                await pg.execute(query, *fixed_row)
                copied += 1

            except Exception as e:

                skipped += 1
                print("   skip row:", str(e)[:80])

        print(f"   copied: {copied} | skipped: {skipped}")

    await pg.close()
    sqlite_conn.close()

    print("\nMigration finished ✔")


if __name__ == "__main__":
    asyncio.run(migrate())