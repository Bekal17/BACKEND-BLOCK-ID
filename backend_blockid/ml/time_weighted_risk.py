"""
Time-weighted risk penalty — decay penalty by age of risk event.
"""
from __future__ import annotations

import math
from typing import List, Dict

_DECAY_HALFLIFE_DAYS = 180  # ~6 months


def time_weighted_penalty(weight: int, days_old: int) -> int:
    """
    Adjust penalty based on age of event.
    Formula: adjusted = weight * exp(-days / 180)
    Recent events (days_old=0) keep full weight; older events decay.
    """
    if days_old is None or days_old <= 0:
        return weight
    adjusted = weight * math.exp(-days_old / _DECAY_HALFLIFE_DAYS)
    return int(adjusted)


def apply_time_weighted_penalties(reasons: List[Dict]) -> int:
    """
    Sum time-weighted penalties from reason codes.
    reasons = [{code, weight, days_old}]
    """
    total = 0
    for r in reasons:
        weight = r.get("weight", 0) or 0
        days_old = r.get("days_old", 0) or 0
        adjusted = time_weighted_penalty(weight, days_old)
        total += adjusted
    return int(total)
