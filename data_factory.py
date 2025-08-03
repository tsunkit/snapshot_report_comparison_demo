from db_source import DatabaseSource
from datafile_source import DataFileSource

class DataFactory:
   
    def get_data_source(config_loader):
       
        source_type = config_loader.get('app', 'datafile_source')

        if source_type == 'datafile':
            csv_file = config_loader.get('datafile', 'csv_file')
            return DataFileSource(csv_file)
        elif source_type == 'database':
            db_url = config_loader.get('database', 'url')
            return DataBaseSource(db_url)
        else:
            raise ValueError("Invalid data source")
