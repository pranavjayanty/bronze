import logging
import os
from pathlib import Path
from typing import Optional, Any, Type
from dotenv import load_dotenv
import sqlalchemy as sa
from sqlalchemy.engine import Engine
import pandas as pd
import asyncio

class Pipeline:
    """Base pipeline class with common functionality."""
    
    def __init__(self, 
                 schema: str = 'bronze',
                 log_level: int = logging.INFO):
        """
        Initialize the pipeline.
        
        Args:
            schema: Target schema name
            log_level: Logging level
        """
        self.setup_logging(log_level)
        self.schema = schema
        self.engine = self._create_db_engine()
        
    def _create_db_engine(self) -> Engine:
        """Create SQLAlchemy engine from environment variables."""
        load_dotenv()
        db_connection = os.getenv("DATABASE_URL")
        if not db_connection:
            raise ValueError("DATABASE_URL must be set in .env file")
        return sa.create_engine(db_connection)
        
    def setup_logging(self, log_level: int) -> None:
        """Configure logging."""
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def execute_ddl(self, ddl_path: str) -> None:
        """
        Execute DDL script.
        
        Args:
            ddl_path: Path to DDL script
        """
        try:
            with open(ddl_path, 'r') as f:
                ddl = f.read()
            
            with self.engine.connect() as conn:
                conn.execute(sa.text(ddl))
                conn.commit()
                
            self.logger.info(f"Successfully executed DDL: {ddl_path}")
            
        except Exception as e:
            self.logger.error(f"Error executing DDL {ddl_path}: {str(e)}")
            raise
            
    def write_dataframe(self, 
                       df: pd.DataFrame,
                       table_name: str,
                       if_exists: str = 'append') -> None:
        """
        Write DataFrame to database.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
        """
        try:
            df.to_sql(
                table_name,
                self.engine,
                schema=self.schema,
                if_exists=if_exists,
                index=False
            )
            self.logger.info(f"Successfully wrote data to {self.schema}.{table_name}")
            
        except Exception as e:
            self.logger.error(f"Error writing to {table_name}: {str(e)}")
            raise

def get_ddl_path(ddl_filename: str) -> str:
    """
    Get the absolute path to a DDL file.
    
    Args:
        ddl_filename: Name of the DDL file (e.g., 'create_bronze_table.sql')
        
    Returns:
        str: Absolute path to the DDL file
    """
    return str(Path(__file__).parent.parent / 'ddls' / ddl_filename)

def run_pipeline(extractor_class: Type,
                ddl_filename: str,
                table_name: str,
                input_path: str) -> None:
    """
    Run a complete pipeline with the given extractor, DDL, and table.
    
    Args:
        extractor_class: The extractor class to use
        ddl_filename: Name of the DDL file to execute
        table_name: Name of the target table
        input_path: Path to the input file
    """
    # Initialize pipeline
    pipeline = Pipeline(schema='bronze')
    
    # Initialize extractor
    extractor = extractor_class()
    extractor.logger = pipeline.logger
    
    try:
        # Execute DDL
        ddl_path = get_ddl_path(ddl_filename)
        pipeline.execute_ddl(ddl_path)
        
        # Extract and transform data
        df = extractor.parse(input_path)
        
        # Load data
        pipeline.write_dataframe(
            df=df,
            table_name=table_name,
            if_exists='append'
        )
        
        pipeline.logger.info("Pipeline completed successfully")
        
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {str(e)}")
        raise 



# BRONZE DISCORD "DML"

from extractors.discord_extractor import DiscordExtractor
from pipeline import Pipeline
import asyncio
load_dotenv()
DARCY_KEY = os.getenv('DARCY_KEY')
TEST_SERVER_ID = os.getenv('TEST_SERVER_ID')
DATABASE_URL = os.getenv('DATABASE_URL')

# DISCORD CHANNELS --------------------------------------------------------------------- */
discord_channels_extractor = DiscordExtractor(DARCY_KEY, TEST_SERVER_ID)
discord_channels_pipeline = Pipeline(
    ddl_filename = 'create_discord_channels_table.sql',
    table_name = 'discord_channels',
    engine = _create_db_engine(),
    recreate_table = True
)

# Follows an ETL process
raw_data = asyncio.run(discord_channels_extractor.fetch_discord_channels()) # Extract
if discord_channels_extractor.recreate_table:
    discord_channels_pipeline.create_table() # Transform
discord_channels_pipeline.run_ingestor_from_df(asyncio.run(discord_channels_extractor.parse_discord_data(raw_data))) # Load
discord_channels_pipeline.test_run_status()

# DISCORD CHAT --------------------------------------------------------------------- */
discord_chat_extractor = DiscordExtractor(DARCY_KEY, TEST_SERVER_ID)
discord_chat_pipeline = Pipeline(
    ddl_filename = 'create_discord_chat_table.sql',
    table_name = 'discord_chat',
    engine = _create_db_engine(),
    recreate_table = True
)

raw_data = asyncio.run(discord_chat_extractor.fetch_discord_chat()) # Extract
if discord_chat_extractor.recreate_table:
    discord_chat_pipeline.create_table() # Transform                            
discord_chat_pipeline.run_ingestor_from_df(asyncio.run(discord_chat_extractor.parse_discord_data(raw_data))) # Load
discord_chat_pipeline.test_run_status()
