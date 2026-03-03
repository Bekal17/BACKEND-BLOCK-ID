ALTER TABLE wallet_reasons ADD COLUMN code TEXT;
ALTER TABLE wallet_reasons ADD COLUMN weight REAL DEFAULT 0;
ALTER TABLE wallet_reasons ADD COLUMN confidence REAL DEFAULT 1;
ALTER TABLE wallet_reasons ADD COLUMN tx_hash TEXT;
ALTER TABLE wallet_reasons ADD COLUMN solscan TEXT;
