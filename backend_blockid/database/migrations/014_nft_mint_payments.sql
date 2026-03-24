CREATE TABLE IF NOT EXISTS nft_mint_payments (
    id SERIAL PRIMARY KEY,
    wallet VARCHAR(64) NOT NULL,
    tx_signature VARCHAR(128) NOT NULL UNIQUE,
    amount_sol NUMERIC(18, 9) NOT NULL,
    mint_address VARCHAR(64),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nft_mint_payments_wallet ON nft_mint_payments(wallet);
CREATE INDEX IF NOT EXISTS idx_nft_mint_payments_tx ON nft_mint_payments(tx_signature);
