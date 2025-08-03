from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from data_source_base import DataSourceBase


class DatabaseSource():
    def __init__(self, db_url: str):
        if not db_url:
            raise ValueError("Database URL must not be empty.")
        self.db_url = db_url

    def connect_data(self):
        self.engine = None
        try:
            self.engine = create_engine(self.db_url)
            # Test connection
            with engine.connect() as conn:
                self.engine = conn.execute("SELECT 1")
            
        except OperationalError as e:
            raise RuntimeError(f"Failed to connect to database: {e}")

    def get_engine(self):
        return self.engine
