"""
Unit test for trust score PDA derivation. Ensures Python PDA matches Anchor seeds:
seeds = [b"trust_score", wallet.key().as_ref()]

To verify against Anchor: run `anchor build` then check on-chain PDA for a known wallet.
"""

from __future__ import annotations


def test_get_trust_score_pda_deterministic():
    """Python PDA derivation is deterministic and uses correct seeds (no oracle)."""
    from solders.pubkey import Pubkey

    from backend_blockid.oracle.solana_publisher import get_trust_score_pda

    program_id = Pubkey.from_string("9ZP3uKj28ridPZbrC9xh7x7eoBaa54PMEp9itmQkwKNW")
    wallet = Pubkey.from_string("8zoFgCTRJXQv82XRmioTNaxDz48Yxrw93UCGJHRKijJ5")

    pda1 = get_trust_score_pda(program_id, wallet)
    pda2 = get_trust_score_pda(program_id, wallet)
    assert pda1 == pda2
    assert str(pda1)  # valid base58


def test_get_trust_score_pda_different_wallets_different_pdas():
    """Different wallets produce different PDAs."""
    from solders.pubkey import Pubkey

    from backend_blockid.oracle.solana_publisher import get_trust_score_pda

    program_id = Pubkey.from_string("9ZP3uKj28ridPZbrC9xh7x7eoBaa54PMEp9itmQkwKNW")
    wallet1 = Pubkey.from_string("8zoFgCTRJXQv82XRmioTNaxDz48Yxrw93UCGJHRKijJ5")
    wallet2 = Pubkey.from_string("9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka")

    pda1 = get_trust_score_pda(program_id, wallet1)
    pda2 = get_trust_score_pda(program_id, wallet2)
    assert pda1 != pda2


def test_derive_trust_score_pda_seeds_format():
    """Seeds are [b'trust_score', bytes(wallet)] - no oracle."""
    from solders.pubkey import Pubkey

    from backend_blockid.oracle.solana_publisher import derive_trust_score_pda

    program_id = Pubkey.from_string("9ZP3uKj28ridPZbrC9xh7x7eoBaa54PMEp9itmQkwKNW")
    wallet = Pubkey.from_string("8zoFgCTRJXQv82XRmioTNaxDz48Yxrw93UCGJHRKijJ5")

    pda, bump = derive_trust_score_pda(wallet, program_id)
    # Manually verify seeds match Anchor: [b"trust_score", wallet.key().as_ref()]
    expected_pda, _ = Pubkey.find_program_address(
        [b"trust_score", bytes(wallet)],
        program_id,
    )
    assert pda == expected_pda
    assert 0 <= bump <= 255
