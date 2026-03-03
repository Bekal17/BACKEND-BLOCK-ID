ALTER TABLE trust_scores ADD COLUMN dynamic_risk REAL DEFAULT 0;
ALTER TABLE trust_scores ADD COLUMN final_score REAL DEFAULT 0;
ALTER TABLE trust_scores ADD COLUMN risk_level INTEGER DEFAULT 0;
ALTER TABLE trust_scores ADD COLUMN last_updated INTEGER;
