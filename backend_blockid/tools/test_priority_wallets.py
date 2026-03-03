from __future__ import annotations

from backend_blockid.ai_engine.priority_wallets import (
    add_wallet,
    get_cluster_neighbors,
    get_priority_wallets,
    update_priority,
)


def main() -> None:
    # Insert sample wallets
    add_wallet("TEST_WALLET_A", "SCAM", hop=0)
    add_wallet("TEST_WALLET_B", "WATCHLIST", hop=0)
    add_wallet("TEST_WALLET_C", "NORMAL", hop=0)
    update_priority("TEST_WALLET_C", 5)

    # Generate neighbors (if transactions table exists)
    neighbors = get_cluster_neighbors("TEST_WALLET_A", max_hop=2)
    print("Neighbors:", neighbors)

    # Fetch top wallets
    top = get_priority_wallets(limit=100)
    print("Top wallets:", top[:10])
    print("Priority wallets selected:", len(top))
    print("Total priority wallets:", len(get_priority_wallets(limit=1000)))


if __name__ == "__main__":
    main()
