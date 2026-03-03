ALTER TABLE priority_wallets ADD COLUMN hop_distance INTEGER DEFAULT 0;
ALTER TABLE priority_wallets ADD COLUMN last_tx_time INTEGER;
ALTER TABLE priority_wallets ADD COLUMN tx_count INTEGER DEFAULT 0;
