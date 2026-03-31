-- Add displayed_badges to social_profiles
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'social_profiles') THEN
        ALTER TABLE social_profiles ADD COLUMN IF NOT EXISTS displayed_badges TEXT[] DEFAULT '{}';
    END IF;
END $$;

