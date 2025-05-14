-- Create a table for storing Discord chat history
CREATE TABLE IF NOT EXISTS bronze.discord_channels (
    id SERIAL PRIMARY KEY,
    channel_id BIGINT NOT NULL UNIQUE,
    channel_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    position INTEGER,
    category VARCHAR(255)
);

-- Index for fast lookup by channel_id
CREATE INDEX IF NOT EXISTS idx_discord_channels_channel_id ON bronze.discord_channels(channel_id); 