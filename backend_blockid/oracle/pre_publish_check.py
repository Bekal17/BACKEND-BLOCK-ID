import os

from backend_blockid.database.pg_connection import get_conn, release_conn

TEST_MODE = os.getenv("BLOCKID_TEST_MODE", "0") == "1"


async def check_test_wallets():
    """
    Stop publish if test wallets exist and TEST_MODE is off.
    """

    conn = await get_conn()
    try:
        rows = await conn.fetch("""
            SELECT wallet
            FROM trust_scores
            WHERE is_test_wallet = 1
        """)

        if rows:
            print("TEST wallets detected in DB:")
            for r in rows[:20]:
                print("   ", r["wallet"])

            if not TEST_MODE:
                raise Exception(
                    "STOP: Test wallets exist but TEST_MODE=0. Cleanup required!"
                )
            else:
                print("TEST_MODE=1 → publish will skip test wallets.")
        else:
            print("No test wallets found.")
    finally:
        await release_conn(conn)
