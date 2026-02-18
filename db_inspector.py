from sqlalchemy import create_engine, inspect
from config import DATABASE_URL

class DBInspector:
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        self.inspector = inspect(self.engine)

    def get_table_schema(self, table_name):
        """Returns column names and types for a table."""
        columns = self.inspector.get_columns(table_name)
        return {col['name']: col['type'] for col in columns}

    def get_foreign_keys(self, table_name):
        """Returns foreign key relationships to maintain integrity."""
        return self.inspector.get_foreign_keys(table_name)

    def get_all_tables(self):
        return self.inspector.get_table_names()