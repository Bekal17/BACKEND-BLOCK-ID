"""
Priority queue scheduler for the agent runtime.

Picks wallets for each cycle by priority rules (deterministic):
- critical wallets → analyzed every cycle
- watchlist wallets → analyzed every 2 cycles
- normal wallets → analyzed every N cycles

Skips wallets analyzed too recently. Returns ordered list (critical, watchlist, normal).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

PRIORITY_CRITICAL = "critical"
PRIORITY_WATCHLIST = "watchlist"
PRIORITY_NORMAL = "normal"

# Default: watchlist every 2 cycles, normal every 4 cycles
DEFAULT_WATCHLIST_EVERY_N_CYCLES = 2
DEFAULT_NORMAL_EVERY_N_CYCLES = 4
DEFAULT_CYCLE_INTERVAL_SEC = 30.0
DEFAULT_MAX_WALLETS_PER_CYCLE = 2000


@dataclass
class PrioritySchedulerConfig:
    """Config for priority-based cycle scheduling."""

    cycle_interval_sec: float = DEFAULT_CYCLE_INTERVAL_SEC
    max_wallets_per_cycle: int = DEFAULT_MAX_WALLETS_PER_CYCLE
    watchlist_every_n_cycles: int = DEFAULT_WATCHLIST_EVERY_N_CYCLES
    normal_every_n_cycles: int = DEFAULT_NORMAL_EVERY_N_CYCLES


def select_wallets_for_cycle(
    db: Any,
    cycle_number: int,
    *,
    now_ts: int | None = None,
    config: PrioritySchedulerConfig | None = None,
) -> list[str]:
    """
    Return ordered list of wallet IDs to analyze this cycle (deterministic).

    Rules:
    - critical: include every cycle (no recency skip).
    - watchlist: include when (cycle_number % watchlist_every_n_cycles == 0),
      skip if last_analyzed_at within watchlist_every_n_cycles * cycle_interval_sec.
    - normal: include when (cycle_number % normal_every_n_cycles == 0),
      skip if last_analyzed_at within normal_every_n_cycles * cycle_interval_sec.

    Order: critical first, then watchlist, then normal (stable sort by wallet within tier).
    """
    cfg = config or PrioritySchedulerConfig()
    now_ts = now_ts if now_ts is not None else int(time.time())
    interval_sec = max(1.0, cfg.cycle_interval_sec)

    rows = db.get_tracked_wallets_with_priority_and_analyzed(limit=50_000)
    critical_min_elapsed = 0
    watchlist_min_elapsed = int(cfg.watchlist_every_n_cycles * interval_sec)
    normal_min_elapsed = int(cfg.normal_every_n_cycles * interval_sec)

    include_watchlist = (cycle_number % cfg.watchlist_every_n_cycles) == 0
    include_normal = (cycle_number % cfg.normal_every_n_cycles) == 0

    critical: list[str] = []
    watchlist: list[str] = []
    normal: list[str] = []

    for wallet, priority, last_analyzed_at in rows:
        priority = (priority or PRIORITY_NORMAL).lower()
        last_ts = last_analyzed_at if last_analyzed_at is not None else 0
        elapsed = now_ts - last_ts if last_ts else 999_999

        if priority == PRIORITY_CRITICAL:
            if elapsed >= critical_min_elapsed:
                critical.append(wallet)
        elif priority == PRIORITY_WATCHLIST:
            if include_watchlist and elapsed >= watchlist_min_elapsed:
                watchlist.append(wallet)
        else:
            if include_normal and elapsed >= normal_min_elapsed:
                normal.append(wallet)

    critical.sort()
    watchlist.sort()
    normal.sort()
    selected = critical + watchlist + normal
    selected = selected[: cfg.max_wallets_per_cycle]

    n_crit = len([w for w in selected if w in set(critical)])
    n_watch = len([w for w in selected if w in set(watchlist)])
    n_norm = len([w for w in selected if w in set(normal)])
    logger.info(
        "priority_scheduler_cycle",
        cycle=cycle_number,
        critical_count=n_crit,
        watchlist_count=n_watch,
        normal_count=n_norm,
        total_selected=len(selected),
        include_watchlist=include_watchlist,
        include_normal=include_normal,
    )
    return selected
