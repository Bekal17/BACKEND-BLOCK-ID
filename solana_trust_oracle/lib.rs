//! Trust Oracle — Anchor-compatible interface spec only.
//! No deployment; use as the contract surface for the Solana program.

use anchor_lang::prelude::*;

declare_id!("TRUSTxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");

#[program]
pub mod trust_oracle {
    use super::*;

    /// Updates the on-chain trust score and risk level for a wallet.
    /// Callable only by the account's oracle authority.
    pub fn update_trust_score(
        ctx: Context<UpdateTrustScore>,
        trust_score: u8,
        risk_level: RiskLevel,
    ) -> Result<()> {
        let account = &mut ctx.accounts.trust_score_account;
        require!(trust_score <= 100, TrustOracleError::InvalidTrustScore);
        account.wallet_pubkey = ctx.accounts.wallet.key();
        account.trust_score = trust_score;
        account.risk_level = risk_level;
        account.last_updated = Clock::get()?.unix_timestamp;
        account.oracle_pubkey = ctx.accounts.oracle.key();
        Ok(())
    }

    /// Read-only: client fetches trust_score_account via RPC; no state change.
    /// Exposed so IDL includes the "get" entrypoint; implementation is no-op.
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
    pub wallet_pubkey: Pubkey,
    /// Score 0–100.
    pub trust_score: u8,
    /// Risk level enum (u8).
    pub risk_level: RiskLevel,
    /// Unix timestamp (seconds) of last update.
    pub last_updated: i64,
    /// Oracle authority that may update this account.
    pub oracle_pubkey: Pubkey,
}

impl TrustScoreAccount {
    pub const LEN: usize = 32 + 1 + 1 + 8 + 32; // 74
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum RiskLevel {
    #[default]
    Low = 0,      // score >= 70
    Medium = 1,   // 50 <= score < 70
    High = 2,     // 30 <= score < 50
    Critical = 3, // score < 30
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

    /// CHECK: Oracle authority; must sign. Constraint: must match trust_score_account.oracle_pubkey after first init.
    #[account(mut)]
    pub oracle: Signer<'info>,

    /// CHECK: Wallet pubkey; used for PDA seeds and stored in account.
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
    #[msg("Trust score must be 0–100")]
    InvalidTrustScore,
    #[msg("Only the oracle authority may update")]
    UnauthorizedOracle,
}
