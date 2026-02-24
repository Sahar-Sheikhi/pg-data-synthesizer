import json
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from config import DATABASE_URL

TARGET_DB_NAME = "synthetic_dev_db"
BASE_URL = DATABASE_URL.rsplit('/', 1)[0]
DESTINATION_DB_URL = f"{BASE_URL}/{TARGET_DB_NAME}"

def create_database_if_not_exists():
    maintenance_url = f"{BASE_URL}/postgres"
    maintenance_engine = create_engine(maintenance_url, isolation_level="AUTOCOMMIT")
    with maintenance_engine.connect() as conn:
        query = text("SELECT 1 FROM pg_database WHERE datname = :db_name")
        exists = conn.execute(query, {"db_name": TARGET_DB_NAME}).scalar()
        if not exists:
            print(f"Database '{TARGET_DB_NAME}' not found. Creating it now...")
            conn.execute(text(f"CREATE DATABASE {TARGET_DB_NAME}"))
        else:
            print(f"Database '{TARGET_DB_NAME}' already exists.")
    maintenance_engine.dispose()

def load_synthetic_data():
    try:
        create_database_if_not_exists()
    except Exception as e:
        print(f"Failed to verify/create database: {e}")
        return

    # Connect to source database to get schema
    source_engine = create_engine(DATABASE_URL)
    dest_engine = create_engine(DESTINATION_DB_URL)
    source_inspector = inspect(source_engine)
    
    try:
        with open("synthetic_foundation.json", "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("Error: synthetic_foundation.json not found.")
        return

    print(f"--- Restoring Synthetic Foundation to: {TARGET_DB_NAME} ---")

    # Get metadata from source database
    source_metadata = MetaData()
    source_metadata.reflect(bind=source_engine)
    
    # Calculate dependency order (same logic as main.py)
    def calculate_dependency_score(table_name):
        if table_name not in source_metadata.tables:
            return 0
        table = source_metadata.tables[table_name]
        fk_count = len([fk for fk in table.foreign_keys])
        return fk_count
    
    # Sort tables by dependency (fewer FKs first)
    table_order = sorted(data.keys(), key=calculate_dependency_score)
    
    print(f"Loading {len(table_order)} tables in dependency order...")
    
    # Drop all custom types first to avoid "already exists" errors
    with dest_engine.begin() as connection:
        result = connection.execute(text("""
            SELECT typname FROM pg_type 
            WHERE typtype = 'e' AND typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
        """))
        for row in result:
            try:
                connection.execute(text(f"DROP TYPE IF EXISTS {row[0]} CASCADE"))
            except:
                pass

    for table_name in table_order:
        rows = data[table_name]
        if not rows:
            continue

        print(f"Restoring table: {table_name} (FK count: {calculate_dependency_score(table_name)})...")
        
        try:
            # Get the table schema from source database
            if table_name in source_metadata.tables:
                source_table = source_metadata.tables[table_name]
                
                with dest_engine.begin() as connection:
                    # Drop table if exists (CASCADE drops dependent objects)
                    connection.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                    
                    # Create table with the same schema as source
                    source_table.create(connection)
                    
                    # Insert data in batches
                    batch_size = 500
                    columns = [col.name for col in source_table.columns]
                    
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i:i+batch_size]
                        
                        # Build parameterized INSERT  
                        placeholders = ', '.join([f':{col}' for col in columns])
                        insert_stmt = f"INSERT INTO {table_name} ({', '.join([f'\"{c}\"' for c in columns])}) VALUES ({placeholders})"
                        
                        connection.execute(text(insert_stmt), batch)
                    
                print(f"   ✅ {len(rows):,} rows synchronized.")
            else:
                print(f"   ⚠️  Table {table_name} not found in source schema, skipping...")
                
        except Exception as e:
            print(f"   ❌ Failed to load {table_name}: {str(e)[:200]}")

    source_engine.dispose()
    dest_engine.dispose()
    print(f"\n✅ Database Restore Complete: {TARGET_DB_NAME}")

if __name__ == "__main__":
    load_synthetic_data()