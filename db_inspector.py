"""
Database Schema Inspector

This module provides database introspection capabilities for analyzing table structures,
foreign key relationships, primary keys, and column metadata. All SQL queries are logged
for audit compliance purposes.

Supports PostgreSQL-specific features including enum type inspection.
"""

import logging
from sqlalchemy import create_engine, inspect, event, text
from config import DATABASE_URL

# Configure SQL query audit logging to track all database interactions
logging.basicConfig(
    filename='audit_log.txt',
    level=logging.INFO,
    format='%(asctime)s - SQL_QUERY: %(message)s'
)


class DBInspector:
    """
    Database schema inspector for extracting metadata and relationships.
    
    This class provides methods to inspect database schema including tables,
    columns, primary keys, foreign keys, and enum types. All SQL queries
    are automatically logged for compliance auditing.
    
    """
    
    def __init__(self):
        """Initialize the database inspector and configure query logging."""
        self.engine = create_engine(DATABASE_URL)
        
        # Register event listener for SQL query audit logging
        @event.listens_for(self.engine, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            """Log all SQL queries executed against the database."""
            logging.info(statement)

        self.inspector = inspect(self.engine)

    def get_all_tables(self):
        """
        Retrieve all table names from the database.
        
        Returns:
            list: A list of table names as strings
        """
        return self.inspector.get_table_names()

    def get_columns(self, table_name):
        """
        Retrieve detailed column metadata for a specific table.
        
        Args:
            table_name (str): Name of the table to inspect
            
        Returns:
            list: List of dictionaries containing column metadata including
                  name, type, nullable, default, etc.
        """
        return self.inspector.get_columns(table_name)
    
    def get_table_row_count(self, table_name):
        """
        Retrieve the number of rows in a table.
        
        This reads only metadata (cardinality) without accessing actual data values,
        ensuring zero data leakage while maintaining realistic table proportions.
        
        Args:
            table_name (str): Name of the table to count
            
        Returns:
            int: Number of rows in the table
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.scalar()
        except Exception as e:
            logging.error(f"Error counting rows for {table_name}: {e}")
            return 1000  # Fallback to default if count fails

    def get_real_foreign_keys(self, table_name):
        """
        Retrieve foreign key constraints from database metadata.
        
        Queries the information_schema to extract actual foreign key relationships,
        ensuring synthetic data generation respects referential integrity.
        
        Args:
            table_name (str): Name of the table to inspect
            
        Returns:
            dict: Mapping of foreign key column names to their referenced tables
                  Example: {'customer_id': 'customer', 'product_id': 'product'}
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
                    fk_metadata[row[0]] = row[1]
        except Exception as e:
            logging.error(f"Error fetching foreign keys for {table_name}: {e}")
            
        return fk_metadata

    def get_primary_keys(self, table_name):
        """
        Retrieve primary key column names for a table.
        
        Args:
            table_name (str): Name of the table to inspect
            
        Returns:
            list: List of primary key column names
        """
        return self.inspector.get_pk_constraint(table_name)['constrained_columns']
    
    def get_enum_values(self, table_name, column_name):
        """
        Retrieve allowed enum values for a column from the database schema.
        
        Note: This method is PostgreSQL-specific and queries the pg_enum catalog.
        Returns an empty list for non-enum columns or if the query fails.
        
        Args:
            table_name (str): Name of the table containing the enum column
            column_name (str): Name of the enum column
            
        Returns:
            list: List of allowed enum values as strings, or empty list if not applicable
        """
        try:
            with self.engine.connect() as conn:
                # Query PostgreSQL enum type definition
                query = text("""
                    SELECT e.enumlabel
                    FROM pg_type t 
                    JOIN pg_enum e ON t.oid = e.enumtypid  
                    JOIN pg_class c ON c.reltype = t.oid
                    JOIN pg_attribute a ON a.attrelid = c.oid AND a.atttypid = t.oid
                    WHERE c.relname = :table AND a.attname = :column
                    ORDER BY e.enumsortorder;
                """)
                result = conn.execute(query, {"table": table_name, "column": column_name})
                return [row[0] for row in result]
        except Exception:
            return []