"""
Seed handle_reserved table with initial reserved handles.
Run after migration 014_handle_registry.sql.
"""
from __future__ import annotations

import asyncio
import os
import sys

# Add project root for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from backend_blockid.database.pg_connection import get_conn, init_db, release_conn

RESERVED_HANDLES = [
    {"handle": "vitalik", "reserved_for": "Vitalik Buterin", "category": "crypto_founder"},
    {"handle": "cz", "reserved_for": "Changpeng Zhao", "category": "crypto_founder"},
    {"handle": "solana", "reserved_for": "Solana Foundation", "category": "protocol"},
    {"handle": "metaplex", "reserved_for": "Metaplex", "category": "protocol"},
    {"handle": "blockid", "reserved_for": "BlockID Team", "category": "team"},
    {"handle": "admin", "reserved_for": "BlockID Admin", "category": "system"},
    {"handle": "support", "reserved_for": "BlockID Support", "category": "system"},
]


async def main() -> None:
    await init_db()
    conn = await get_conn()
    try:
        for r in RESERVED_HANDLES:
            await conn.execute(
                """
                INSERT INTO handle_reserved (handle, reserved_for, category)
                VALUES ($1, $2, $3)
                ON CONFLICT (handle) DO UPDATE SET reserved_for = $2, category = $3
                """,
                r["handle"],
                r["reserved_for"],
                r["category"],
            )
            print(f"  Reserved: @{r['handle']} for {r['reserved_for']}")
        print("Done. Reserved handles seeded.")
    finally:
        await release_conn(conn)


if __name__ == "__main__":
    asyncio.run(main())
