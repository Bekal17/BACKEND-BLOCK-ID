//! BlockID Oracle — Anchor program for on-chain trust scores.
//! Oracle authority updates TrustScoreAccount per wallet.

use anchor_lang::prelude::*;

declare_id!("55iMY3uHQadPv4PXwqF1uYWdyie3wqKCwJHs97eWPE6B");

#[program]
pub mod blockid_oracle {
    use super::*;

    pub fn update_trust_score(
        ctx: Context<UpdateTrustScore>,
        score: u8,
        risk: u8,
    ) -> Result<()> {
        let acct = &mut ctx.accounts.trust_score_account;

        acct.wallet = ctx.accounts.wallet.key();
        acct.score = score;
        acct.risk = risk;
        acct.updated_at = Clock::get()?.unix_timestamp;

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
}

impl TrustScoreAccount {
    pub const LEN: usize = 32 + 1 + 1 + 8;
}

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

    #[account(mut)]
    pub oracle: Signer<'info>,

    /// CHECK: only used for PDA seed
    pub wallet: UncheckedAccount<'info>,

    pub system_program: Program<'info, System>,
}
