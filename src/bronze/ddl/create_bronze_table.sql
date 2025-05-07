-- Create a sample bronze table for raw data ingestion
CREATE TABLE IF NOT EXISTS bronze.sample_data (
    id SERIAL PRIMARY KEY,
    source_file VARCHAR(255),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB,
    metadata JSONB,
    status VARCHAR(50) DEFAULT 'pending'
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_sample_data_source_file ON bronze.sample_data(source_file);
CREATE INDEX IF NOT EXISTS idx_sample_data_status ON bronze.sample_data(status);
CREATE INDEX IF NOT EXISTS idx_sample_data_ingestion_timestamp ON bronze.sample_data(ingestion_timestamp); 
