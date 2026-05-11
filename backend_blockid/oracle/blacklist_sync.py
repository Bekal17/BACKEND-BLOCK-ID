import asyncio
import json
import time

import aiohttp

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)

ALLENHARK_URL = "https://allenhark.com/blacklist.jsonl"
SOURCE_LABEL = "allenhark"


async def sync_allenhark_blacklist() -> dict:
    """
    Downloads allenhark.com scammer blacklist and upserts into scam_wallets.
    Returns summary of sync operation.
    """
    logger.info("blacklist_sync_start", source=SOURCE_LABEL)

    downloaded = 0
    inserted = 0
    skipped = 0
    errors = 0

    try:
        # Download blacklist
        async with aiohttp.ClientSession() as session:
            async with session.get(
                ALLENHARK_URL,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    logger.error(
                        "blacklist_sync_download_failed",
                        status=resp.status,
                        source=SOURCE_LABEL,
                    )
                    return {"success": False, "error": f"HTTP {resp.status}"}

                content = await resp.text()

        # Parse JSONL
        wallets = []
        for line in content.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                addr = entry.get("addr", "").strip()
                ts = entry.get("ts", 0)
                if addr and len(addr) >= 32:
                    wallets.append({
                        "wallet": addr,
                        "detected_at": int(ts / 1000) if ts > 1e10 else int(ts),
                    })
                    downloaded += 1
            except (json.JSONDecodeError, ValueError):
                errors += 1
                continue

        logger.info(
            "blacklist_sync_parsed",
            downloaded=downloaded,
            errors=errors,
            source=SOURCE_LABEL,
        )

        if not wallets:
            return {"success": False, "error": "No wallets parsed"}

        # Upsert into scam_wallets
        conn = await get_conn()
        try:
            # Reset sequence to prevent primary key conflicts
            await conn.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence('scam_wallets', 'id'),
                    COALESCE((SELECT MAX(id) FROM scam_wallets), 0) + 1,
                    false
                )
                """
            )

            # Batch upsert in chunks of 500 for performance
            CHUNK_SIZE = 500
            for i in range(0, len(wallets), CHUNK_SIZE):
                chunk = wallets[i:i + CHUNK_SIZE]
                try:
                    await conn.executemany(
                        """
                        INSERT INTO scam_wallets
                            (wallet, source, label, detected_at,
                             confidence_score, notes)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (wallet) DO UPDATE SET
                            source = EXCLUDED.source,
                            label = EXCLUDED.label,
                            detected_at = EXCLUDED.detected_at,
                            confidence_score = EXCLUDED.confidence_score,
                            notes = EXCLUDED.notes
                        """,
                        [
                            (
                                w["wallet"],
                                SOURCE_LABEL,
                                "rug_pull_launcher",
                                w["detected_at"] or int(time.time()),
                                0.85,
                                "High-frequency pump.fun rug pull launcher. Source: allenhark.com",
                            )
                            for w in chunk
                        ],
                    )
                    inserted += len(chunk)
                except Exception as e:
                    logger.warning(
                        "blacklist_sync_batch_error",
                        chunk_start=i,
                        error=str(e),
                    )
                    errors += len(chunk)
        finally:
            await release_conn(conn)

        summary = {
            "success": True,
            "downloaded": downloaded,
            "inserted": inserted,
            "skipped": skipped,
            "errors": errors,
            "source": SOURCE_LABEL,
            "synced_at": int(time.time()),
        }
        logger.info("blacklist_sync_complete", **summary)
        return summary

    except aiohttp.ClientError as e:
        logger.error("blacklist_sync_network_error", error=str(e))
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error("blacklist_sync_unexpected_error", error=str(e))
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    result = asyncio.run(sync_allenhark_blacklist())
    print(result)
