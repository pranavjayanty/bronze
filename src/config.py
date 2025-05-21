import os
from dotenv import load_dotenv
import sqlalchemy as sa

def get_db_engine():
    load_dotenv()
    db_url = os.getenv('DATABASE_URL')
    return sa.create_engine(db_url)
