import json
import random
from sqlalchemy import create_engine, text, inspect
from config import DATABASE_URL

# --- CONFIGURATION ---
SAMPLE_SIZE = 1  # Number of tables to sample and compare
SYNTHETIC_DB = "synthetic_dev_db"
# ---------------------

def compare_databases():
    # Connect to both databases
    source_db = DATABASE_URL.rsplit('/', 1)[1]
    base_url = DATABASE_URL.rsplit('/', 1)[0]
    synthetic_url = f"{base_url}/{SYNTHETIC_DB}"
    
    source_engine = create_engine(DATABASE_URL)
    synthetic_engine = create_engine(synthetic_url)
    
    print(f"🔍 COMPARING DATABASES")
    print(f"=" * 80)
    print(f"Source DB:     {source_db}")
    print(f"Synthetic DB:  {SYNTHETIC_DB}")
    print(f"=" * 80)
    
    # Get all tables from source
    source_inspector = inspect(source_engine)
    all_tables = source_inspector.get_table_names()
    
    # Randomly select tables to compare
    tables_to_check = random.sample(all_tables, min(len(all_tables), SAMPLE_SIZE))
    
    for table_name in tables_to_check:
        print(f"\n📊 TABLE: {table_name.upper()}")
        print("-" * 80)
        
        try:
            # Get columns
            columns = [col['name'] for col in source_inspector.get_columns(table_name)]
            
            # Fetch one random record from source database
            with source_engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT 1"))
                source_row = result.fetchone()
            
            # Fetch one random record from synthetic database
            with synthetic_engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT 1"))
                synthetic_row = result.fetchone()
            
            if source_row and synthetic_row:
                print(f"\n{'Column Name':<25} | {'Source DB Value':<30} | {'Synthetic DB Value':<30} | {'Type Match'}")
                print("-" * 120)
                
                for i, col_name in enumerate(columns):
                    source_val = source_row[i]
                    synthetic_val = synthetic_row[i]
                    
                    # Check type match
                    source_type = type(source_val).__name__
                    synthetic_type = type(synthetic_val).__name__
                    type_match = "✅" if source_type == synthetic_type else f"❌ ({source_type} vs {synthetic_type})"
                    
                    # Truncate long values for display
                    source_str = str(source_val)[:28] + ".." if len(str(source_val)) > 30 else str(source_val)
                    synthetic_str = str(synthetic_val)[:28] + ".." if len(str(synthetic_val)) > 30 else str(synthetic_val)
                    
                    print(f"{col_name:<25} | {source_str:<30} | {synthetic_str:<30} | {type_match}")
                
                print(f"\n✅ Sample comparison complete for {table_name}")
            else:
                print(f"⚠️  No data found in one or both databases for {table_name}")
                
        except Exception as e:
            print(f"❌ Error comparing {table_name}: {e}")
    
    source_engine.dispose()
    synthetic_engine.dispose()
    
    print(f"\n{'=' * 80}")
    print(f"✅ Database comparison complete!")

if __name__ == "__main__":
    compare_databases()