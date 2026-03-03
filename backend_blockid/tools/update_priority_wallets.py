from __future__ import annotations

from backend_blockid.ai_engine.priority_wallets import (
    age_priorities,
    boost_active_wallets,
    populate_priority_wallets,
    remove_old_wallets,
)


def main() -> None:
    remove_old_wallets(days=30)
    populate_priority_wallets()
    aged = age_priorities()
    boosted = boost_active_wallets()
    print(f"Aging applied to {aged} wallets")
    print(f"Boosted active wallets: {boosted}")


if __name__ == "__main__":
    main()
