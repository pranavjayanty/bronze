CREATE TABLE bronze_user_table (
  id                   BIGSERIAL PRIMARY KEY,
  name                 TEXT     NOT NULL,
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
  last_edited_time     TIMESTAMP WITH TIME ZONE,
  ingestion_timestamp  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
