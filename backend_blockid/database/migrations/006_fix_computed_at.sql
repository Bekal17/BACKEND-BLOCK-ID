ALTER TABLE trust_scores
ADD COLUMN computed_at INTEGER DEFAULT (strftime('%s','now'));

UPDATE trust_scores
SET computed_at = strftime('%s','now')
WHERE computed_at IS NULL;
