#!/usr/bin/env python3
"""
Script to run the Discord pipeline manually.
This will extract data from Discord and load it into the database.
"""

import os
import asyncio
import pandas as pd
from dotenv import load_dotenv
from bronze.extractors.discord_extractor import DiscordExtractor
from bronze.utils.pipeline import Pipeline

async def run_pipeline():
    print("Starting Discord pipeline...")
    
    # Ensure environment variables are loaded
    load_dotenv()
    
    # Verify required environment variables
    required_vars = ["DARCY_KEY", "TEST_SERVER_ID", "DATABASE_URL"]
    for var in required_vars:
        if not os.getenv(var):
            print(f"Error: {var} environment variable must be set")
            return
    
    # Initialize pipeline
    pipeline = Pipeline(schema='bronze')
    
    # Initialize extractor
    extractor = DiscordExtractor()
    extractor.logger = pipeline.logger
    
    try:
        # Execute DDL to create the table
        ddl_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'bronze', 'ddl', 'create_discord_chat_table.sql')
        print(f"Executing DDL from {ddl_path}...")
        pipeline.execute_ddl(ddl_path)
        
        # Extract and transform data
        print("Extracting data from Discord...")
        df = await extractor.parse()
        
        # Print data summary
        print(f"\nExtracted {len(df)} records from Discord")
        print("\nChannel distribution:")
        print(df.groupby("channel_name").size())
        
        # Load data into database
        print("\nLoading data into database...")
        pipeline.write_dataframe(
            df=df,
            table_name='discord_chat_table',
            if_exists='append'
        )
        
        # Verify data was loaded
        with pipeline.engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM bronze.discord_chat_table").scalar()
            print(f"\nSuccessfully loaded {result} records into the database")
            
            # Show sample data
            print("\nSample data from database:")
            sample = conn.execute("""
                SELECT channel_name, author, content, created_at
                FROM bronze.discord_chat_table 
                ORDER BY created_at DESC
                LIMIT 5
            """).fetchall()
            
            for row in sample:
                print(f"Channel: {row[0]}, Author: {row[1]}, Time: {row[3]}")
                print(f"Content: {row[2][:100]}..." if len(row[2]) > 100 else f"Content: {row[2]}")
                print("-" * 50)
        
        print("\nPipeline completed successfully!")
        
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(run_pipeline()) 