import os

from backend_blockid.database.connection import get_connection

TEST_MODE = os.getenv("BLOCKID_TEST_MODE", "0") == "1"


def check_test_wallets():
    """
    Stop publish if test wallets exist and TEST_MODE is off.
    """

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT wallet
        FROM trust_scores
        WHERE is_test_wallet = 1
    """)

    rows = cur.fetchall()
    conn.close()

    if rows:
        print("TEST wallets detected in DB:")
        for r in rows[:20]:
            print("   ", r[0])

        if not TEST_MODE:
            raise Exception(
                "STOP: Test wallets exist but TEST_MODE=0. Cleanup required!"
            )
        else:
            print("TEST_MODE=1 → publish will skip test wallets.")
    else:
        print("No test wallets found.")
