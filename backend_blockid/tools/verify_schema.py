from __future__ import annotations

from backend_blockid.database.connection import get_connection


def table_exists(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def column_exists(conn, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def verify_schema() -> None:
    conn = get_connection()

    required = {
        "trust_scores": ["wallet", "ml_score", "dynamic_risk", "final_score", "risk_level"],
        "transactions": ["sender", "receiver", "signature"],
        "priority_wallets": ["wallet", "priority", "reason"],
        "wallet_reasons": ["wallet", "code"],
    }

    for table, cols in required.items():
        if not table_exists(conn, table):
            conn.close()
            raise Exception(f"Missing table: {table}. Run migrations to fix schema.")

        for c in cols:
            if not column_exists(conn, table, c):
                conn.close()
                raise Exception(f"Missing column {c} in {table}. Run migrations to fix schema.")

    conn.close()
    print("✔ BlockID DB schema verified.")


if __name__ == "__main__":
    verify_schema()
