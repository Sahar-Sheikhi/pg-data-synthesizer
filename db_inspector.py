import logging
from sqlalchemy import create_engine, inspect, event, text
from config import DATABASE_URL

# Configure SQL query audit logging
logging.basicConfig(filename='audit_log.txt', level=logging.INFO, 
                    format='%(asctime)s - SQL_QUERY: %(message)s')

class DBInspector:
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        
        # SQL query audit logging
        @event.listens_for(self.engine, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            # Log all SQL queries for compliance auditing
            logging.info(statement)

        self.inspector = inspect(self.engine)

    def get_all_tables(self):
        """Returns a list of all table names in the database."""
        return self.inspector.get_table_names()

    def get_columns(self, table_name):
        """Returns detailed column metadata for a specific table."""
        return self.inspector.get_columns(table_name)

    def get_real_foreign_keys(self, table_name):
        """
        Retrieves foreign key constraints from database metadata.
        Ensures synthetic data matches the actual database schema.
        """
        fk_metadata = {}
        try:
            with self.engine.connect() as conn:
                # Query foreign key relationships from information schema
                query = text("""
                    SELECT 
                        kcu.column_name, 
                        ccu.table_name AS referred_table
                    FROM information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY' 
                      AND tc.table_name = :table;
                """)
                result = conn.execute(query, {"table": table_name})
                for row in result:
                    # Map foreign key column to referenced table
                    fk_metadata[row[0]] = row[1]
        except Exception as e:
            logging.error(f"Error fetching FKs for {table_name}: {e}")
            
        return fk_metadata

    def get_primary_keys(self, table_name):
        """Returns primary key columns for the specified table."""
        return self.inspector.get_pk_constraint(table_name)['constrained_columns']