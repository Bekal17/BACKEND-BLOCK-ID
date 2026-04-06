-- Add link preview columns to social_posts
ALTER TABLE social_posts ADD COLUMN IF NOT EXISTS link_url TEXT;
ALTER TABLE social_posts ADD COLUMN IF NOT EXISTS link_title TEXT;
ALTER TABLE social_posts ADD COLUMN IF NOT EXISTS link_description TEXT;
ALTER TABLE social_posts ADD COLUMN IF NOT EXISTS link_image TEXT;
