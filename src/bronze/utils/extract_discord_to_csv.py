#!/usr/bin/env python3
"""
Script to extract Discord chat data and save it directly to CSV.
"""

import os
import asyncio
import pandas as pd
from dotenv import load_dotenv
from bronze.extractors.discord_extractor import DiscordExtractor

async def extract_to_csv(output_path: str = "discord_chat_data.csv"):
    print("Starting Discord data extraction...")
    
    # Ensure environment variables are loaded
    load_dotenv()
    
    # Verify required environment variables
    required_vars = ["DARCY_KEY", "TEST_SERVER_ID"]
    for var in required_vars:
        if not os.getenv(var):
            print(f"Error: {var} environment variable must be set")
            return
    
    # Initialize extractor
    extractor = DiscordExtractor()
    
    try:
        # Extract data
        print("Extracting data from Discord...")
        df = await extractor.parse()
        
        # Print data summary
        print(f"\nExtracted {len(df)} records from Discord")
        print("\nChannel distribution:")
        print(df.groupby("channel_name").size())
        
        # Save to CSV
        print(f"\nSaving data to {output_path}...")
        df.to_csv(output_path, index=False)
        
        print("\nData extraction completed successfully!")
        print(f"CSV file saved to: {output_path}")
        
        # Show sample data
        print("\nSample data:")
        print(df[["channel_name", "author", "content", "created_at"]].head())
        
    except Exception as e:
        print(f"Extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(extract_to_csv()) 