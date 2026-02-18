import json
from db_inspector import DBInspector
from data_generator import DataGenerator
from config import ROW_COUNT

def run_synthesis():
    # Initialize the tools
    inspector = DBInspector()
    generator = DataGenerator()
    
    print(f"--- 🚀 Starting Universal Synthesis for {ROW_COUNT} rows per table ---")
    
    # 1. Dynamic Discovery: Get all table names automatically
    all_tables = inspector.get_all_tables()
    print(f"Detected {len(all_tables)} tables in database.")

    synthetic_foundation = {}

    # 2. Iterate through every table and generate data based on its schema
    for table_name in all_tables:
        print(f"Processing table: {table_name}...")
        
        try:
            # Get column metadata from the Inspector (Name and Type)
            columns_metadata = inspector.get_columns(table_name)
            
            # Construct the table schema dictionary required by the new generator
            # Format: {'column_name': DataTypeObject}
            table_schema = {col['name']: col['type'] for col in columns_metadata}
            
            # Call the new generalized generator method
            table_data = generator.generate_table_data(table_name, table_schema, ROW_COUNT)
            
            # Store in our final dictionary
            synthetic_foundation[table_name] = table_data
            
        except Exception as e:
            print(f"⚠️  Could not process table '{table_name}': {e}")

    # 3. Save the Foundation to JSON
    output_file = "synthetic_foundation.json"
    try:
        with open(output_file, "w") as f:
            # default=str handles non-serializable objects like Dates or Decimals
            json.dump(synthetic_foundation, f, indent=4, default=str)
        
        total_records = sum(len(rows) for rows in synthetic_foundation.values())
        print("\n" + "="*50)
        print(f"✅ SUCCESS: Universal Synthetic Foundation created!")
        print(f"📁 File: {output_file}")
        print(f"📊 Total Records Generated: {total_records}")
        print(f"📑 Proof: Check 'audit_log.txt' for zero-read verification.")
        print("="*50)
        
    except Exception as e:
        print(f"❌ Error saving output: {e}")

if __name__ == "__main__":
    run_synthesis()