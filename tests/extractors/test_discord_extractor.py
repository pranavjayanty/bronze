import os
import asyncio
import pytest
import pandas as pd
from dotenv import load_dotenv
from bronze.extractors.discord_extractor import DiscordExtractor

@pytest.mark.asyncio
async def test_discord_extractor_initialization():
    """Test that the Discord extractor initializes correctly."""
    # Ensure environment variables are loaded
    load_dotenv()
    
    # Verify environment variables are set
    token = os.getenv("DARCY_KEY")
    guild_id = os.getenv("TEST_SERVER_ID")
    assert token is not None, "DARCY_KEY environment variable must be set"
    assert guild_id is not None, "TEST_SERVER_ID environment variable must be set"
    
    # Initialize extractor
    extractor = DiscordExtractor()
    assert extractor.token == token
    assert extractor.guild_id == int(guild_id)

@pytest.mark.asyncio
async def test_fetch_channels():
    """Test fetching channels from Discord."""
    extractor = DiscordExtractor()
    channels_df = await extractor.fetch_channels()
    
    # Verify DataFrame structure
    assert isinstance(channels_df, pd.DataFrame)
    assert not channels_df.empty, "No channels were fetched"
    
    # Verify expected columns
    expected_columns = ["channel_name", "channel_id", "created_at"]
    for column in expected_columns:
        assert column in channels_df.columns, f"Column {column} missing from result"
    
    print(f"Successfully fetched {len(channels_df)} channels")

@pytest.mark.asyncio
async def test_fetch_chat_history():
    """Test fetching chat history from Discord."""
    extractor = DiscordExtractor()
    messages_df = await extractor.fetch_chat_history()
    
    # Verify DataFrame structure
    assert isinstance(messages_df, pd.DataFrame)
    
    # Note: Messages might be empty if the server is new or has no messages
    if not messages_df.empty:
        # Verify expected columns
        expected_columns = [
            "channel_name", "channel_id", "thread_name", "thread_id", 
            "message_id", "author", "author_id", "content", 
            "created_at", "edited_at", "is_thread"
        ]
        for column in expected_columns:
            assert column in messages_df.columns, f"Column {column} missing from result"
        
        print(f"Successfully fetched {len(messages_df)} messages")

@pytest.mark.asyncio
async def test_parse_method():
    """Test the main parse method of the extractor."""
    extractor = DiscordExtractor()
    result_df = await extractor.parse()
    
    # Verify DataFrame structure and metadata columns
    assert isinstance(result_df, pd.DataFrame)
    assert 'ingestion_timestamp' in result_df.columns
    assert 'source' in result_df.columns
    assert result_df['source'].iloc[0] == 'discord_extractor'
    
    print(f"Successfully parsed {len(result_df)} records with metadata")

if __name__ == "__main__":
    # Run the tests directly for manual testing
    asyncio.run(test_discord_extractor_initialization())
    asyncio.run(test_fetch_channels())
    asyncio.run(test_fetch_chat_history())
    asyncio.run(test_parse_method())
    print("All manual tests completed successfully!") 