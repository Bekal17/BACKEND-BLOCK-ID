CREATE INDEX IF NOT EXISTS idx_priority
ON priority_wallets(priority DESC);

CREATE INDEX IF NOT EXISTS idx_last_checked
ON priority_wallets(last_checked);

CREATE INDEX IF NOT EXISTS idx_tx_sender
ON transactions(sender);

CREATE INDEX IF NOT EXISTS idx_tx_receiver
ON transactions(receiver);
