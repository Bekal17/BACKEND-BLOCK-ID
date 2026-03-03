from __future__ import annotations

from backend_blockid.database.connection import get_connection


def _ensure_column(cur, table: str, column: str, col_type: str) -> None:
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    except Exception:
        pass


def main() -> None:
    conn = get_connection()
    cur = conn.cursor()

    # Ensure trust_scores columns
    _ensure_column(cur, "trust_scores", "ml_score", "REAL")
    _ensure_column(cur, "trust_scores", "dynamic_risk", "REAL")
    _ensure_column(cur, "trust_scores", "final_score", "REAL")
    _ensure_column(cur, "trust_scores", "risk_level", "INTEGER")
    _ensure_column(cur, "trust_scores", "last_updated", "INTEGER")

    conn.commit()
    conn.close()
    print("Dynamic Risk v2 migration complete")


if __name__ == "__main__":
    main()
