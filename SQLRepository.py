import os

class SQLRepository:
    def __init__(self, sql_dir):
        self.sql_dir = sql_dir
        self.queries = {}
        self._load_all_sql_files()

    def _load_all_sql_files(self):
        for filename in os.listdir(self.sql_dir):
            if filename.endswith('.sql'):
                path = os.path.join(self.sql_dir, filename)
                with open(path, 'r') as file:
                    self.queries[filename] = file.read().strip()

    def get(self, filename):
        if filename not in self.queries:
            raise ValueError(f"SQL file '{filename}' not found.")
        return self.queries[filename]

    def all(self):
        return self.queries
