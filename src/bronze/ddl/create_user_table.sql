CREATE TABLE IF NOT EXISTS bronze.user_table (
    id                   SERIAL PRIMARY KEY,
    name                 TEXT,
    role                 TEXT,
    status               TEXT,
    team                 TEXT,
    joined               TEXT,
    bio                  TEXT,
    email                TEXT,
    discord_tag          TEXT,
    facebook             TEXT,
    instagram            TEXT,
    linkedin             TEXT,
    working_on           TEXT,
    workload             TEXT,
    last_edited_time     TIMESTAMP,                 
    ingestion_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX IF NOT EXISTS idx_user_table_name                ON bronze.user_table(name);
CREATE INDEX IF NOT EXISTS idx_user_table_status              ON bronze.user_table(status);
CREATE INDEX IF NOT EXISTS idx_user_table_team                ON bronze.user_table(team);
CREATE INDEX IF NOT EXISTS idx_user_table_last_edited_time    ON bronze.user_table(last_edited_time);
CREATE INDEX IF NOT EXISTS idx_user_table_ingestion_timestamp ON bronze.user_table(ingestion_timestamp);
