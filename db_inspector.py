import logging
from sqlalchemy import create_engine, inspect, event
from config import DATABASE_URL

# Set up a dedicated logger for SQL Audit
logging.basicConfig(filename='audit_log.txt', level=logging.INFO, 
                    format='%(asctime)s - SQL_QUERY: %(message)s')

class DBInspector:
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        
        # --- THE AUDIT LOG LOGIC ---
        @event.listens_for(self.engine, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            # This captures every single query sent to Postgres
            logging.info(statement)
        # ---------------------------

        self.inspector = inspect(self.engine)

    def get_all_tables(self):
        return self.inspector.get_table_names()

    def get_columns(self, table_name):
        return self.inspector.get_columns(table_name)