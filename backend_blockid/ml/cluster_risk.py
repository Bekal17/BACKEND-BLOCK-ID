import math


def cluster_risk_penalty(
    scam_penalty,
    distance,
    days_old,
    confidence=1.0,
    decay=1.5,
):
    """
    Compute propagated penalty from scam wallet in cluster.
    """

    distance_factor = math.exp(-distance / decay)
    time_weight = math.exp(-days_old / 180)

    penalty = scam_penalty * distance_factor * time_weight * confidence

    return penalty

