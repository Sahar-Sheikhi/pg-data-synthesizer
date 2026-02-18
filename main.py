import json
from db_inspector import DBInspector
from data_generator import DataGenerator
from config import ROW_COUNT

def run_synthesis():
    # 1. Initialize our components
    inspector = DBInspector()
    generator = DataGenerator()
    
    print(f"--- Starting Synthesis for {ROW_COUNT} rows ---")
    
    # 2. Inspect Schema (Zero Data Export Principle)
    # We learn the columns to ensure our dictionary keys match the DB
    tables = inspector.get_all_tables()
    print(f"Targeting tables: {', '.join(tables[:5])}...")

    # 3. Generate Data in Order (Integrity First)
    # We MUST generate customers before rentals so the IDs exist
    print("Generating Synthetic Customers...")
    customers = generator.generate_customer(ROW_COUNT)
    
    print("Generating Synthetic Rentals (Linking to Customers)...")
    rentals = generator.generate_rental(ROW_COUNT)

    # 4. Save the "Local Foundation"
    output_data = {
        "customer": customers,
        "rental": rentals
    }
    
    with open("synthetic_foundation.json", "w") as f:
        # Use default=str to handle datetime objects from Faker
        json.dump(output_data, f, indent=4, default=str)
    
    print("\n[SUCCESS] Foundation created: synthetic_foundation.json")

if __name__ == "__main__":
    run_synthesis()