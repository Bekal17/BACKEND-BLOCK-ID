//! BlockID Oracle — Anchor program for on-chain trust scores.
//! Oracle authority updates TrustScoreAccount per wallet.

use anchor_lang::prelude::*;

declare_id!("55iMY3uHQadPv4PXwqF1uYWdyie3wqKCwJHs97eWPE6B");

#[program]
pub mod blockid_oracle {
    use super::*;

    /// Updates the on-chain trust score and risk for a wallet.
    /// Callable only by the oracle authority (signer).
    pub fn update_trust_score(
        ctx: Context<UpdateTrustScore>,
        wallet: Pubkey,
        score: u8,
        risk: u8,
    ) -> Result<()> {
        require!(ctx.accounts.wallet.key() == wallet, TrustOracleError::InvalidWallet);
        require!(score <= 100, TrustOracleError::InvalidTrustScore);
        require!(risk <= 3, TrustOracleError::InvalidRisk);

        let account = &mut ctx.accounts.trust_score_account;
        account.wallet = wallet;
        account.score = score;
        account.risk = risk;
        account.updated_at = Clock::get()?.unix_timestamp;

        Ok(())
    }

    /// Read-only: client fetches trust_score_account via RPC; no state change.
    pub fn get_trust_score(_ctx: Context<GetTrustScore>) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Account structure
// ---------------------------------------------------------------------------

#[account]
#[derive(Default)]
pub struct TrustScoreAccount {
    /// Wallet whose trust score is stored.
    pub wallet: Pubkey,
    /// Score 0–100.
    pub score: u8,
    /// Risk level 0–3 (e.g. Low=0, Medium=1, High=2, Critical=3).
    pub risk: u8,
    /// Unix timestamp (seconds) of last update.
    pub updated_at: i64,
}

impl TrustScoreAccount {
    pub const LEN: usize = 32 + 1 + 1 + 8; // 42
}

// ---------------------------------------------------------------------------
// Instruction contexts
// ---------------------------------------------------------------------------

#[derive(Accounts)]
pub struct UpdateTrustScore<'info> {
    #[account(
        init_if_needed,
        payer = oracle,
        space = 8 + TrustScoreAccount::LEN,
        seeds = [b"trust_score", oracle.key().as_ref(), wallet.key().as_ref()],
        bump
    )]
    pub trust_score_account: Account<'info, TrustScoreAccount>,

    /// Oracle authority; must sign to update scores.
    #[account(mut)]
    pub oracle: Signer<'info>,

    /// Wallet pubkey; used for PDA seeds and must match instruction wallet arg.
    /// CHECK: Used only for PDA derivation and key comparison.
    pub wallet: UncheckedAccount<'info>,

    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct GetTrustScore<'info> {
    #[account(
        seeds = [b"trust_score", oracle.key().as_ref(), wallet.key().as_ref()],
        bump
    )]
    pub trust_score_account: Account<'info, TrustScoreAccount>,

    /// CHECK: Oracle pubkey (used for PDA derivation).
    pub oracle: UncheckedAccount<'info>,

    /// CHECK: Wallet pubkey (used for PDA derivation).
    pub wallet: UncheckedAccount<'info>,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[error_code]
pub enum TrustOracleError {
    #[msg("Wallet account must match instruction wallet pubkey")]
    InvalidWallet,
    #[msg("Trust score must be 0–100")]
    InvalidTrustScore,
    #[msg("Risk must be 0–3")]
    InvalidRisk,
    #[msg("Only the oracle authority may update")]
    UnauthorizedOracle,
}
