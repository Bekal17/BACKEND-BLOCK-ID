"""
Aggregate and deduplicate reason codes, normalize trust scores.

Fixes: duplicate reason codes, wrong penalty sign, scores not 0-100,
UPSERT not replacing, inconsistent API aggregation.
"""
from __future__ import annotations

import sys
import time
from collections import defaultdict
from pathlib import Path

if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from backend_blockid.database.connection import get_connection

MAX_NEGATIVE = -50
MAX_POSITIVE = 40
NO_RISK_CODE = "NO_RISK_DETECTED"


def _score_to_risk(score: float) -> str:
    if score >= 80:
        return "0"
    if score >= 50:
        return "1"
    if score >= 20:
        return "2"
    return "3"


def main() -> int:
    conn = get_connection()
    cur = conn.cursor()

    for col, typ in [("confidence_score", "REAL"), ("created_at", "INTEGER")]:
        try:
            cur.execute(f"ALTER TABLE wallet_reasons ADD COLUMN {col} {typ}")
        except Exception:
            pass

    cur.execute("PRAGMA table_info(wallet_reasons)")
    cols = {row[1] for row in cur.fetchall()}
    has_confidence = "confidence_score" in cols

    if has_confidence:
        cur.execute(
            """
            SELECT wallet, reason_code, weight, confidence_score
            FROM wallet_reasons
            WHERE wallet IS NOT NULL AND reason_code IS NOT NULL
            """
        )
    else:
        cur.execute(
            """
            SELECT wallet, reason_code, weight
            FROM wallet_reasons
            WHERE wallet IS NOT NULL AND reason_code IS NOT NULL
            """
        )
    raw = cur.fetchall()
    original_count = len(raw)

    grouped: dict[str, dict[str, tuple[float, float]]] = defaultdict(lambda: defaultdict(lambda: (0.0, 0.0)))
    for r in raw:
        w = (r["wallet"] if hasattr(r, "keys") else r[0]).strip()
        code = (r["reason_code"] if hasattr(r, "keys") else r[1]).strip()
        wt = int(r["weight"] if hasattr(r, "keys") else r[2] or 0)
        conf = float((r["confidence_score"] if hasattr(r, "keys") else r[3]) or 0) if has_confidence else 0.0
        prev_wt, prev_conf = grouped[w][code]
        grouped[w][code] = (prev_wt + wt, max(prev_conf, conf))

    deduped_count = 0
    for w, codes in grouped.items():
        deduped_count += len(codes)

    duplicates_removed = original_count - deduped_count

    cur.execute("SELECT wallet, score FROM trust_scores WHERE wallet IS NOT NULL")
    base_scores = {}
    for r in cur.fetchall():
        w = (r["wallet"] if hasattr(r, "keys") else r[0]).strip()
        s = float((r["score"] if hasattr(r, "keys") else r[1]) or 50)
        if w:
            base_scores[w] = s

    now_ts = int(time.time())
    penalties_list: list[float] = []
    updates: list[tuple[float, str, str, str]] = []

    for wallet, codes in grouped.items():
        if NO_RISK_CODE in codes and len(codes) > 1:
            codes = {k: v for k, v in codes.items() if k != NO_RISK_CODE}

        total_penalty = 0.0
        for code, (wt, _conf) in codes.items():
            wt = int(wt)
            if wt < 0:
                clamped = max(MAX_NEGATIVE, wt)
            else:
                clamped = min(MAX_POSITIVE, wt)
            total_penalty += clamped

        penalties_list.append(total_penalty)
        base = base_scores.get(wallet, 50.0)
        final = max(0.0, min(100.0, base + total_penalty))
        final = round(final, 2)
        risk = _score_to_risk(final)
        reason_codes_str = ",".join(sorted(codes.keys()))
        if wallet in base_scores:
            updates.append((final, risk, reason_codes_str, wallet))

    for final, risk, reason_codes_str, wallet in updates:
        try:
            cur.execute(
                """
                UPDATE trust_scores
                SET score = ?, risk_level = ?, reason_codes = ?, updated_at = ?
                WHERE wallet = ?
                """,
                (final, risk, reason_codes_str, now_ts, wallet),
            )
        except Exception as e:
            print(f"[aggregate] WARNING: update failed for {wallet[:8]}...: {e}")

    wallets_in_trust = set(base_scores.keys())
    deduped_rows: list[tuple[str, str, int, float, int]] = []
    for wallet, codes in grouped.items():
        if wallet not in wallets_in_trust:
            continue
        if NO_RISK_CODE in codes and len(codes) > 1:
            codes = {k: v for k, v in codes.items() if k != NO_RISK_CODE}
        for code, (wt, conf) in codes.items():
            wt = int(wt)
            if wt < 0:
                clamped = max(MAX_NEGATIVE, wt)
            else:
                clamped = min(MAX_POSITIVE, wt)
            deduped_rows.append((wallet, code, clamped, conf, now_ts))

    cur.execute("DELETE FROM wallet_reasons")
    for wallet, code, wt, conf, ts in deduped_rows:
        try:
            cur.execute(
                """
                INSERT INTO wallet_reasons (wallet, reason_code, weight, confidence_score, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (wallet, code, wt, conf, ts),
            )
        except Exception:
            try:
                cur.execute(
                    """
                    INSERT INTO wallet_reasons (wallet, reason_code, weight, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (wallet, code, wt, ts),
                )
            except Exception:
                pass

    cur.execute(
        "DELETE FROM wallet_reasons WHERE wallet NOT IN (SELECT wallet FROM trust_scores)"
    )

    conn.commit()
    conn.close()

    avg_penalty = sum(penalties_list) / len(penalties_list) if penalties_list else 0.0
    print(f"[aggregate] wallets={len(updates)}")
    print(f"[aggregate] avg_penalty={avg_penalty:.1f}")
    print(f"[aggregate] duplicates_removed={duplicates_removed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
