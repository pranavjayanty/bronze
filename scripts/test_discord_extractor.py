#!/usr/bin/env python3
"""
Simple script to manually test the Discord extractor.
This will fetch channels and message history from Discord and display summary information.
"""

import asyncio
import pandas as pd
from bronze.extractors.discord_extractor import DiscordExtractor

async def test_discord_extractor():
    print("Initializing Discord extractor...")
    extractor = DiscordExtractor()
    
    print("\n---- Testing channel fetching ----")
    channels_df = await extractor.fetch_channels()
    print(f"Successfully fetched {len(channels_df)} channels:")
    print(channels_df)
    
    print("\n---- Testing chat history fetching ----")
    messages_df = await extractor.fetch_chat_history()
    print(f"Successfully fetched {len(messages_df)} messages")
    
    # Print sample of messages (first 5)
    if not messages_df.empty:
        print("\nSample messages:")
        print(messages_df[["channel_name", "author", "content"]].head(5))
        
        # Show message count by channel
        channel_counts = messages_df.groupby("channel_name").size()
        print("\nMessage count by channel:")
        print(channel_counts)
        
        # Show message count by author
        author_counts = messages_df.groupby("author").size().sort_values(ascending=False)
        print("\nMessage count by author (top 5):")
        print(author_counts.head(5))
    else:
        print("No messages found in the Discord server.")

if __name__ == "__main__":
    asyncio.run(test_discord_extractor()) 