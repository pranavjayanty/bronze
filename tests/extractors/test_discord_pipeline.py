import os
import asyncio
import pytest
import pandas as pd
from dotenv import load_dotenv
from bronze.extractors.discord_extractor import DiscordExtractor
from bronze.utils.pipeline import run_pipeline, Pipeline

@pytest.mark.asyncio
async def test_discord_pipeline():
    """Test the full Discord pipeline from extraction to database loading."""
    # Ensure environment variables are loaded
    load_dotenv()
    
    # Verify database connection is available
    db_connection = os.getenv("DATABASE_URL")
    assert db_connection is not None, "DATABASE_URL environment variable must be set"
    
    # Initialize pipeline
    pipeline = Pipeline(schema='bronze')
    
    # Initialize extractor
    extractor = DiscordExtractor()
    extractor.logger = pipeline.logger
    
    try:
        # Execute DDL to create the table
        ddl_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'bronze', 'ddl', 'create_discord_chat_table.sql')
        pipeline.execute_ddl(ddl_path)
        
        # Extract and transform data
        df = await extractor.parse()
        
        # Verify data was extracted
        assert isinstance(df, pd.DataFrame)
        assert not df.empty, "No data was extracted from Discord"
        
        # Verify required columns
        required_columns = [
            "channel_name", "channel_id", "thread_name", "thread_id", 
            "message_id", "author", "author_id", "content", 
            "created_at", "edited_at", "is_thread", "ingestion_timestamp", "source"
        ]
        for column in required_columns:
            assert column in df.columns, f"Column {column} missing from result"
        
        # Load data into database
        pipeline.write_dataframe(
            df=df,
            table_name='discord_chat_table',
            if_exists='append'
        )
        
        # Verify data was loaded by querying the database
        with pipeline.engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM bronze.discord_chat_table").scalar()
            assert result > 0, "No data was loaded into the database"
            
            # Verify a sample record
            sample = conn.execute("""
                SELECT channel_name, author, content 
                FROM bronze.discord_chat_table 
                LIMIT 1
            """).fetchone()
            assert sample is not None, "Could not retrieve sample record from database"
            
        print(f"Successfully loaded {len(df)} records into the database")
        
    except Exception as e:
        pytest.fail(f"Pipeline test failed: {str(e)}")

if __name__ == "__main__":
    # Run the test directly for manual testing
    asyncio.run(test_discord_pipeline())
    print("Pipeline test completed successfully!") 