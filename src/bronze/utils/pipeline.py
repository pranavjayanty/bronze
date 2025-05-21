import logging
import os
from pathlib import Path
from typing import Optional, Any, Type
from dotenv import load_dotenv
import sqlalchemy as sa
from sqlalchemy.engine import Engine
import pandas as pd
import asyncio
from src.config import config

class Pipeline:
    def __init__(self, ddl_filepath: str, table_name: str, engine = _create_db_engine(), recreate_table: bool = True):
        self.ddl_file_path = ddl_filepath
        self.table_name = table_name
        self.engine = engine
        self.recreate_table = recreate_table
    def _create_db_engine(self):
        """calls config.py from src/config.py"""
        return config.get_db_engine()
    def create_table(self):
        with open(self.ddl_file_path, 'r') as f:
            ddl = f.read()
        with self.engine.connect() as conn:
            conn.execute(sa.text(ddl))
            conn.commit()
    def ingest_from_df(self, df: pd.DataFrame):
        df.to_sql(
            self.table_name,
            self.engine,
            schema=self.schema,
            if_exists='replace',
            index=False
        )
    def test_run_status(self):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(sa.text(f"SELECT * FROM {self.table_name} LIMIT 5"))
                for row in result:
                    print(row)
        except Exception as e:
            return f"Failed to run test run status: {e}"

