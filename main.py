import json
from db_inspector import DBInspector
from data_generator import DataGenerator
from config import ROW_COUNT

def run_synthesis():
    inspector = DBInspector()
    generator = DataGenerator()
    
    print(f"--- Starting Synthesis for {ROW_COUNT} rows ---")
    
    # 1. Inspect (Zero Data Export)
    tables = inspector.get_all_tables()
    print(f"Detected tables: {', '.join(tables[:5])}...")

    # 2. Generate
    print("Generating Customers...")
    customers = generator.generate_customer(ROW_COUNT)
    
    print("Generating Rentals (linked to Customers)...")
    rentals = generator.generate_rental(ROW_COUNT)

    # 3. Save Foundation
    output = {
        "customers": customers,
        "rentals": rentals
    }
    
    with open("synthetic_foundation.json", "w") as f:
        json.dump(output, f, indent=4, default=str)
    
    print(f"Success! Generated {len(customers) + len(rentals)} records.")

if __name__ == "__main__":
    run_synthesis()