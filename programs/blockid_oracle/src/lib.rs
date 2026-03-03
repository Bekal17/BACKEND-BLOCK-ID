//! BlockID Oracle — Anchor program for on-chain trust scores.
//! Mainnet-ready version with security checks and future-proof design.

use anchor_lang::prelude::*;
use std::str::FromStr;

declare_id!("9gmL8o1iW3Vrd51jKGkdSjupM3R4DfF9Ni2BhtqYkwng");

// 👉 CHANGE THIS BEFORE MAINNET DEPLOY
pub const TRUSTED_ORACLE: &str = "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka";

#[program]
pub mod blockid_oracle {
    use super::*;

    pub fn update_trust_score(
        ctx: Context<UpdateTrustScore>,
        score: u8,
        risk: u8,
    ) -> Result<()> {

        // -----------------------------
        // 1️⃣ Oracle authorization
        // -----------------------------
        require_keys_eq!(
            ctx.accounts.oracle.key(),
            Pubkey::from_str(TRUSTED_ORACLE).unwrap(),
            ErrorCode::UnauthorizedOracle
        );

        // -----------------------------
        // 2️⃣ Validate inputs
        // -----------------------------
        require!(score <= 100, ErrorCode::InvalidScore);
        require!(risk <= 5, ErrorCode::InvalidRisk);

        let acct = &mut ctx.accounts.trust_score_account;

        acct.wallet = ctx.accounts.wallet.key();
        acct.score = score;
        acct.risk = risk;
        acct.updated_at = Clock::get()?.unix_timestamp;
        acct.oracle = ctx.accounts.oracle.key();
        acct.version = 1;

        Ok(())
    }
}

#[account]
#[derive(Default)]
pub struct TrustScoreAccount {
    pub wallet: Pubkey,
    pub score: u8,
    pub risk: u8,
    pub updated_at: i64,
    pub oracle: Pubkey,

    // 👉 future-proof fields
    pub version: u8,
    pub reserved: [u8; 32],
}

impl TrustScoreAccount {
    // account size calculation
    pub const LEN: usize =
        32 + // wallet
        1 +  // score
        1 +  // risk
        8 +  // updated_at
        32 + // oracle
        1 +  // version
        32;  // reserved
}

#[derive(Accounts)]
pub struct UpdateTrustScore<'info> {

    #[account(
        init_if_needed,
        payer = oracle,
        space = 8 + TrustScoreAccount::LEN,
        seeds = [b"trust_score", wallet.key().as_ref()],
        bump
    )]
    pub trust_score_account: Account<'info, TrustScoreAccount>,

    #[account(mut)]
    pub oracle: Signer<'info>,

    /// CHECK: wallet address only used as seed and stored as Pubkey
    pub wallet: UncheckedAccount<'info>,

    pub system_program: Program<'info, System>,
}

#[error_code]
pub enum ErrorCode {
    #[msg("Unauthorized oracle")]
    UnauthorizedOracle,

    #[msg("Invalid wallet")]
    InvalidWallet,

    #[msg("Invalid score (must be 0-100)")]
    InvalidScore,

    #[msg("Invalid risk (must be 0-5)")]
    InvalidRisk,
}