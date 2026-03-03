"""
Graph-based risk penalty — hop distance from known scam wallets.
"""

from __future__ import annotations

# Penalty by hop distance: 0 = in scam cluster, 1 = 1 hop away, etc.
_DISTANCE_PENALTIES = {
    0: -100,
    1: -60,
    2: -30,
    3: -10,
}


def graph_distance_penalty(distance: int) -> int:
    """
    Penalty based on hop distance from scam wallet.

    Args:
        distance: Hop distance (0 = wallet is scam, 1 = direct neighbor, etc.).

    Returns:
        Penalty to apply (0 or negative). Distances > 3 yield 0.
    """
    if distance is None or distance < 0:
        return 0
    return _DISTANCE_PENALTIES.get(distance, 0)


def apply_graph_penalty(score: int, distance: int) -> tuple[int, int]:
    """
    Apply graph distance penalty to trust score.

    Args:
        score: Pre-penalty trust score.
        distance: Hop distance from scam wallet (999 = unknown/no path).

    Returns:
        (final_score, penalty_applied)
    """
    penalty = graph_distance_penalty(distance)
    final = score + penalty
    final = max(0, min(100, final))
    return final, penalty
