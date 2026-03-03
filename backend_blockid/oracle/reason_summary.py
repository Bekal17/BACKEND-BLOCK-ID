NEGATIVE_MAP = {
    "SCAM_CLUSTER_MEMBER": "connected to known scam cluster",
    "RUG_PULL_SIMULATION": "involved in rug-pull simulation",
    "DRAINER_FLOW_DETECTED": "linked to drainer activity",
    "BLACKLISTED_CREATOR": "created scam token",
}

POSITIVE_MAP = {
    "CLEAN_HISTORY": "no suspicious activity detected",
    "NO_RISK_DETECTED": "wallet appears safe",
}


def build_summary(reasons: list[dict]) -> str:
    negatives = []
    positives = []

    for r in reasons:
        code = r["code"]
        if code in NEGATIVE_MAP:
            negatives.append(NEGATIVE_MAP[code])
        elif code in POSITIVE_MAP:
            positives.append(POSITIVE_MAP[code])

    if negatives:
        return "Wallet " + ", ".join(negatives) + "."

    if positives:
        return "Wallet " + ", ".join(positives) + "."

    return "No significant activity detected."