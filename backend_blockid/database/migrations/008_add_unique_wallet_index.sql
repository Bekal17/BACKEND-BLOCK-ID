DELETE FROM trust_scores
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM trust_scores
    GROUP BY wallet
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_trust_scores_wallet
ON trust_scores(wallet);
