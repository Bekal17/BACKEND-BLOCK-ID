-- Add language hint column for content_violations (analytics only)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'content_violations'
      AND column_name = 'language'
  ) THEN
    ALTER TABLE content_violations ADD COLUMN language TEXT DEFAULT 'unknown';
  END IF;
END $$;
