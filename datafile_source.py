import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import Integer, Float, String, text
from config_loader import ConfigLoader
from data_source_base import DataSourceBase

class DataFileSource(DataSourceBase):
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.engine_name=os.path.splitext(csv_file)[0]
        self.db_url = 'sqlite:///{engine_name}.db'
        self.connect_data()

    def connect_data(self):
        df = pd.read_csv(self.csv_file)
        self.engine = create_engine(self.db_url)
        metadata = MetaData()
 
        def infer_type(series):
            if pd.api.types.is_integer_dtype(series):
                return Integer
            elif pd.api.types.is_float_dtype(series):
                return Float
            else:
                return String(255)  # Assume VARCHAR(255) for text columns
 
        columns = []
        for col in df.columns:
            col_type = infer_type(df[col])
            columns.append(Column(col, col_type))      
        
        #create table structure based on csv file
        table = Table(self.engine_name, metadata, *columns)
        
        metadata.create_all(self.engine)
        
        #inserting csv files into sqlalchemy table
        #df.to_sql(self.engine_name, self.engine, if_exists='replace', index=False)
        
        # Insert data using SQLAlchemy instead of pandas to_sql
        with self.engine.connect() as connection:
            # Drop the table if it exists to ensure a clean slate
            table.drop(self.engine, checkfirst=True)
            # Recreate the table
            metadata.create_all(self.engine)

            # Insert data row by row
            for _, row in df.iterrows():
                insert_stmt = insert(table).values(**row.to_dict())
                connection.execute(insert_stmt)
            connection.commit()
            
    def get_engine(self):
        return self.engine
