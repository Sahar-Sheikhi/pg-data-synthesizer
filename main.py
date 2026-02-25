"""
Database-Agnostic Synthetic Data Generator

This module orchestrates the generation of synthetic data by analyzing database schema,
determining table dependencies, and generating type-safe synthetic records for all tables.
The generated data maintains referential integrity and respects database constraints.
"""

import json
from db_inspector import DBInspector
from data_generator import DataGenerator


def run_synthesis():
    """
    Main synthesis orchestration function.
    
    Analyzes database schema, determines optimal table processing order based on
    foreign key dependencies, and generates synthetic data for all tables while
    maintaining referential integrity.
    
    The generated synthetic data is exported to 'synthetic_foundation.json'.
    """
    # Initialize database inspector and data generator
    inspector = DBInspector()
    generator = DataGenerator(inspector)
    
    print("--- Starting Database-Agnostic Data Synthesis ---")
    
    all_tables = inspector.get_all_tables()
    
    def calculate_foreign_key_count(table_name):
        """
        Calculate the number of foreign keys for a table.
        
        Tables with fewer foreign keys are processed first,
        ensuring parent records exist before child records are generated.
        
        Args:
            table_name (str): Name of the table to analyze
            
        Returns:
            int: Number of foreign key constraints for the table
        """
        fks_map = inspector.get_real_foreign_keys(table_name)
        return len(fks_map)

    print("Analyzing table dependencies...")
    
    # Sort tables by foreign key count and alphabetically for consistency
    sorted_tables = sorted(all_tables, key=lambda t: (calculate_foreign_key_count(t), t))

    synthetic_foundation = {}

    for table_name in sorted_tables:
        foreign_key_count = calculate_foreign_key_count(table_name)
        row_count = inspector.get_table_row_count(table_name)
        print(f"Processing '{table_name}' (Foreign Keys: {foreign_key_count}, Rows: {row_count})...")
        
        try:
            # Retrieve column metadata and construct schema dictionary
            columns_metadata = inspector.get_columns(table_name)
            table_schema = {col['name']: col['type'] for col in columns_metadata}
            
            # Generate synthetic data matching source table cardinality
            table_data = generator.generate_table_data(table_name, table_schema, row_count)
            synthetic_foundation[table_name] = table_data
            
        except Exception as e:
            print(f"WARNING: Skipping '{table_name}': {e}")

    # Export synthetic data to JSON file
    with open("synthetic_foundation.json", "w") as f:
        json.dump(synthetic_foundation, f, indent=4, default=str)
    
    print("\nSynthesis Complete. Table processing order determined by schema analysis.")


if __name__ == "__main__":
    run_synthesis()