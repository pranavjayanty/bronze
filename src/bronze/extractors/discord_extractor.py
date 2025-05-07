import os
import ssl
from typing import List, Dict, Any, Optional
from datetime import datetime

import pandas as pd
import discord
from discord import Client, Intents, TextChannel
from dotenv import load_dotenv

class DiscordExtractor:
    """
    Discord data extractor that:
    - Fetches channel information
    - Fetches all messages and threads
    - Returns data as pandas DataFrames
    """
    
    def __init__(self):
        """Initialize the Discord extractor with configuration and environment variables."""
        # Disable SSL verification
        ssl._create_default_https_context = ssl._create_unverified_context
        
        # Load environment variables
        load_dotenv()
        self.token = os.getenv("DARCY_KEY")
        self.guild_id = int(os.getenv("TEST_SERVER_ID", "0"))
        
        if not self.token or not self.guild_id:
            raise ValueError("DARCY_KEY and TEST_SERVER_ID must be set in .env file")
        
        # Configure intents
        self.intents = Intents.default()
        self.intents.message_content = True
        self.intents.guilds = True
        self.intents.guild_messages = True
    
    def create_client(self) -> Client:
        """Create and return a new Discord client with configured intents."""
        return Client(intents=self.intents)
    
    async def fetch_channels(self) -> pd.DataFrame:
        """Fetch all text channels and return as DataFrame."""
        client = self.create_client()
        channels_data = []
        
        @client.event
        async def on_ready():
            try:
                print("Fetching channels...")
                guild = client.get_guild(self.guild_id)
                if not guild:
                    raise ValueError(f"Guild with ID {self.guild_id} not found")
                
                for channel in guild.text_channels:
                    channels_data.append({
                        "channel_name": channel.name,
                        "channel_id": channel.id,
                        "created_at": channel.created_at.isoformat(),
                        "position": channel.position,
                        "category": channel.category.name if channel.category else None
                    })
                
                print("Channel fetch completed successfully")
                
            except Exception as e:
                print(f"Error fetching channels: {str(e)}")
            finally:
                await client.close()
        
        await client.start(self.token)
        return pd.DataFrame(channels_data)
    
    async def fetch_chat_history(self) -> pd.DataFrame:
        """Fetch all messages and threads and return as DataFrame."""
        client = self.create_client()
        messages_data = []
        
        @client.event
        async def on_ready():
            try:
                print("Fetching chat history...")
                guild = client.get_guild(self.guild_id)
                if not guild:
                    raise ValueError(f"Guild with ID {self.guild_id} not found")
                
                for channel in guild.text_channels:
                    print(f"Processing channel: {channel.name}")
                    
                    # Fetch channel messages
                    async for message in channel.history(limit=None):
                        messages_data.append({
                            "channel_name": channel.name,
                            "channel_id": channel.id,
                            "thread_name": None,
                            "thread_id": None,
                            "message_id": message.id,
                            "author": str(message.author),
                            "author_id": message.author.id,
                            "content": message.content,
                            "created_at": message.created_at.isoformat(),
                            "edited_at": message.edited_at.isoformat() if message.edited_at else None,
                            "is_thread": False
                        })
                    
                    # Fetch and process threads
                    threads = await channel.archived_threads(limit=None)
                    active_threads = channel.threads
                    
                    for thread in [*threads, *active_threads]:
                        print(f"Processing thread: {thread.name}")
                        async for message in thread.history(limit=None):
                            messages_data.append({
                                "channel_name": channel.name,
                                "channel_id": channel.id,
                                "thread_name": thread.name,
                                "thread_id": thread.id,
                                "message_id": message.id,
                                "author": str(message.author),
                                "author_id": message.author.id,
                                "content": message.content,
                                "created_at": message.created_at.isoformat(),
                                "edited_at": message.edited_at.isoformat() if message.edited_at else None,
                                "is_thread": True
                            })
                
                print("Chat history fetch completed successfully")
                
            except Exception as e:
                print(f"Error fetching chat history: {str(e)}")
            finally:
                await client.close()
        
        await client.start(self.token)
        return pd.DataFrame(messages_data)
    
    async def parse(self, input_path: Optional[str] = None) -> pd.DataFrame:
        """
        Main method to fetch and process Discord data.
        The input_path parameter is kept for compatibility but not used.
        
        Args:
            input_path: Optional path parameter (not used in this implementation)
            
        Returns:
            pd.DataFrame: Processed Discord data ready for database insertion
        """
        try:
            # Fetch chat history
            df = await self.fetch_chat_history()
            
            # Add metadata columns
            df['ingestion_timestamp'] = pd.Timestamp.now()
            df['source'] = 'discord_extractor'
            
            return df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error processing Discord data: {str(e)}")
            raise 
