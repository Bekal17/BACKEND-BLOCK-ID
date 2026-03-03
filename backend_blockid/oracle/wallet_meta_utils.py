from datetime import datetime, timezone


def compute_wallet_age_days(first_tx_ts):
    if not first_tx_ts:
        return 0

    now = datetime.now(timezone.utc)
    tx_time = datetime.fromtimestamp(first_tx_ts, tz=timezone.utc)

    return (now - tx_time).days
