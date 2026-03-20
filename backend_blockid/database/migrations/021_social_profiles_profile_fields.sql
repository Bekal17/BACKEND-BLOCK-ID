-- Add profile display fields to social_profiles (if table exists from prior setup)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'social_profiles') THEN
        ALTER TABLE social_profiles ADD COLUMN IF NOT EXISTS display_name TEXT;
        ALTER TABLE social_profiles ADD COLUMN IF NOT EXISTS display_name_source TEXT;
        ALTER TABLE social_profiles ADD COLUMN IF NOT EXISTS bio TEXT;
        ALTER TABLE social_profiles ADD COLUMN IF NOT EXISTS website TEXT;
        ALTER TABLE social_profiles ADD COLUMN IF NOT EXISTS location TEXT;
    END IF;
END
$$;
