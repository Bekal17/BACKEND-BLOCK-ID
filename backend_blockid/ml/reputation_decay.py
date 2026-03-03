def apply_reputation_decay(
    score: int,
    wallet_age_days: int,
    last_scam_days: int,
) -> tuple[int, int]:
    """
    Adjust score based on wallet age and scam recency.
    Returns (final_score, adjustment).
    """
    adjustment = 0

    # -------- GOOD HISTORY --------
    if wallet_age_days > 365:
        adjustment += 20
    elif wallet_age_days > 180:
        adjustment += 10
    elif wallet_age_days > 90:
        adjustment += 5

    # -------- RECENT SCAM --------
    if last_scam_days < 7:
        adjustment -= 30
    elif last_scam_days < 30:
        adjustment -= 15
    elif last_scam_days < 90:
        adjustment -= 5

    final = score + adjustment

    final = max(0, min(100, final))

    return final, adjustment
