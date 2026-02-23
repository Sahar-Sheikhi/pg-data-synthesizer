import json
from db_inspector import DBInspector
from data_generator import DataGenerator
from config import ROW_COUNT

def run_synthesis():
    inspector = DBInspector()
    generator = DataGenerator()
    
    print(f"--- 🚀 Starting Agnostic Synthesis ---")
    
    all_tables = inspector.get_all_tables()
    
    # Automatic dependency ordering:
    # Tables with fewer foreign key columns are processed first to establish parent records
    def calculate_dependency_score(table_name):
        cols = inspector.get_columns(table_name)
        # Count foreign key indicators (columns ending in '_id' excluding own primary key)
        clean_name = table_name.split('.')[-1].lower()
        fk_count = sum(1 for c in cols if "_id" in c['name'].lower() and clean_name not in c['name'].lower())
        return fk_count

    print("Analyzing table dependencies...")
    sorted_tables = sorted(all_tables, key=calculate_dependency_score)

    synthetic_foundation = {}

    for table_name in sorted_tables:
        print(f"Processing '{table_name}' (Score: {calculate_dependency_score(table_name)})...")
        try:
            columns_metadata = inspector.get_columns(table_name)
            table_schema = {col['name']: col['type'] for col in columns_metadata}
            
            # Generate synthetic data for table
            table_data = generator.generate_table_data(table_name, table_schema, ROW_COUNT)
            synthetic_foundation[table_name] = table_data
            
        except Exception as e:
            print(f"⚠️  Skip '{table_name}': {e}")

    with open("synthetic_foundation.json", "w") as f:
        json.dump(synthetic_foundation, f, indent=4, default=str)
    
    print("\n✅ Synthesis Complete. Order automatically determined by schema analysis.")

if __name__ == "__main__":
    run_synthesis()