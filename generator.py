"""
Synthetic Data Generator for dvdrental Database

This script uses SQLAlchemy to inspect the database schema (without reading real data)
and Faker to generate synthetic data while maintaining referential integrity.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from faker import Faker


class SyntheticDataGenerator:
    """
    Generates synthetic data for dvdrental database tables.
    Uses SQLAlchemy inspection to learn schema without exporting real data.
    """
    
    def __init__(self, connection_string: str, records_per_table: int = 50):
        """
        Initialize the generator.
        
        Args:
            connection_string: PostgreSQL connection string
            records_per_table: Number of records to generate per table
        """
        self.connection_string = connection_string
        self.records_per_table = records_per_table
        self.engine: Engine = create_engine(connection_string)
        self.inspector = inspect(self.engine)
        self.faker = Faker()
        
        # In-Memory Registry to track generated IDs for referential integrity
        self.id_registry: Dict[str, List[int]] = {
            'customer': [],
            'film': [],
            'address': [],
            'rental': [],
            'staff': [],
            'inventory': [],
            'store': []
        }
        
        # Schema metadata
        self.schema_info: Dict[str, Any] = {}
        
        # Generated synthetic data
        self.synthetic_data: Dict[str, List[Dict]] = {}
    
    def inspect_schema(self, tables: List[str]) -> Dict[str, Any]:
        """
        Inspect database schema for specified tables without reading real data.
        
        Args:
            tables: List of table names to inspect
            
        Returns:
            Dictionary containing schema information
        """
        print("🔍 Inspecting database schema...")
        
        for table_name in tables:
            columns = self.inspector.get_columns(table_name)
            primary_keys = self.inspector.get_pk_constraint(table_name)
            foreign_keys = self.inspector.get_foreign_keys(table_name)
            
            self.schema_info[table_name] = {
                'columns': columns,
                'primary_keys': primary_keys['constrained_columns'],
                'foreign_keys': foreign_keys
            }
            
            print(f"  ✓ {table_name}: {len(columns)} columns, "
                  f"{len(primary_keys['constrained_columns'])} PKs, "
                  f"{len(foreign_keys)} FKs")
        
        return self.schema_info
    
    def _generate_value_for_column(self, column_name: str, column_type: str, 
                                   is_nullable: bool) -> Any:
        """
        Generate a fake value based on column name and type.
        
        Args:
            column_name: Name of the column
            column_type: SQL type of the column
            is_nullable: Whether column allows NULL
            
        Returns:
            Generated fake value
        """
        # Handle nullable columns
        if is_nullable and self.faker.boolean(chance_of_getting_true=10):
            return None
        
        column_lower = column_name.lower()
        
        # String/VARCHAR types
        if 'VARCHAR' in column_type or 'TEXT' in column_type:
            if 'email' in column_lower:
                return self.faker.email()
            elif 'first_name' in column_lower:
                return self.faker.first_name()
            elif 'last_name' in column_lower:
                return self.faker.last_name()
            elif 'address' in column_lower and column_lower != 'address2':
                return self.faker.street_address()
            elif 'address2' in column_lower:
                return self.faker.secondary_address() if self.faker.boolean(chance_of_getting_true=30) else None
            elif 'district' in column_lower:
                return self.faker.city()
            elif 'phone' in column_lower:
                return self.faker.phone_number()[:20]  # Limit to column size
            elif 'postal' in column_lower or 'zip' in column_lower:
                return self.faker.postcode()
            elif 'city' in column_lower:
                return self.faker.city()
            elif 'country' in column_lower:
                return self.faker.country()
            elif 'title' in column_lower:
                return self.faker.sentence(nb_words=3).rstrip('.')
            elif 'description' in column_lower:
                return self.faker.text(max_nb_chars=200)
            elif 'username' in column_lower:
                return self.faker.user_name()
            elif 'password' in column_lower:
                return self.faker.password()
            else:
                # Extract max length from VARCHAR(n)
                if '(' in column_type:
                    max_len = int(column_type.split('(')[1].split(')')[0])
                    return self.faker.text(max_nb_chars=min(max_len, 50))[:max_len]
                return self.faker.word()
        
        # Integer types
        elif 'INTEGER' in column_type or 'SMALLINT' in column_type:
            if 'year' in column_lower:
                return self.faker.year()
            elif 'rating' in column_lower:
                return self.faker.random_int(min=1, max=5)
            elif 'duration' in column_lower:
                return self.faker.random_int(min=1, max=7)
            elif 'length' in column_lower:
                return self.faker.random_int(min=60, max=180)
            elif 'active' in column_lower:
                return 1 if self.faker.boolean(chance_of_getting_true=90) else 0
            else:
                return self.faker.random_int(min=1, max=100)
        
        # Numeric/Decimal types
        elif 'NUMERIC' in column_type or 'DECIMAL' in column_type:
            if 'amount' in column_lower or 'rate' in column_lower or 'cost' in column_lower:
                return round(self.faker.pyfloat(min_value=0.99, max_value=299.99, right_digits=2), 2)
            return round(self.faker.pyfloat(min_value=0, max_value=1000, right_digits=2), 2)
        
        # Boolean types
        elif 'BOOLEAN' in column_type:
            return self.faker.boolean(chance_of_getting_true=80)
        
        # Date types
        elif 'DATE' in column_type and 'TIME' not in column_type:
            return self.faker.date_between(start_date='-10y', end_date='today').isoformat()
        
        # Timestamp/Datetime types
        elif 'TIMESTAMP' in column_type or 'DATETIME' in column_type:
            return self.faker.date_time_between(start_date='-2y', end_date='now').isoformat()
        
        # Default fallback
        else:
            return self.faker.word()
    
    def _generate_record_for_table(self, table_name: str, record_id: int) -> Dict[str, Any]:
        """
        Generate a single synthetic record for a table.
        
        Args:
            table_name: Name of the table
            record_id: ID for this record
            
        Returns:
            Dictionary representing a synthetic record
        """
        record = {}
        table_schema = self.schema_info[table_name]
        
        for column in table_schema['columns']:
            col_name = column['name']
            col_type = str(column['type'])
            is_nullable = column['nullable']
            
            # Handle primary key
            if col_name in table_schema['primary_keys']:
                record[col_name] = record_id
                continue
            
            # Handle foreign keys
            is_foreign_key = False
            for fk in table_schema['foreign_keys']:
                if col_name in fk['constrained_columns']:
                    is_foreign_key = True
                    referenced_table = fk['referred_table']
                    
                    # Get a valid ID from the registry
                    if referenced_table in self.id_registry and self.id_registry[referenced_table]:
                        record[col_name] = self.faker.random_element(self.id_registry[referenced_table])
                    else:
                        # Fallback if referenced table hasn't been generated yet
                        record[col_name] = self.faker.random_int(min=1, max=10)
                    break
            
            if not is_foreign_key:
                # Generate synthetic value based on column type and name
                record[col_name] = self._generate_value_for_column(col_name, col_type, is_nullable)
        
        return record
    
    def generate_fake_data(self, tables: List[str]) -> Dict[str, List[Dict]]:
        """
        Generate synthetic data for specified tables.
        
        Args:
            tables: List of table names to generate data for
            
        Returns:
            Dictionary mapping table names to lists of synthetic records
        """
        print(f"\n🎲 Generating {self.records_per_table} synthetic records per table...")
        
        # Generation order matters for referential integrity
        # Generate in dependency order: address -> customer, film -> rental
        ordered_tables = ['address', 'customer', 'film', 'rental']
        
        # Only generate tables that are both requested and in our ordered list
        tables_to_generate = [t for t in ordered_tables if t in tables]
        
        # Add any remaining requested tables
        for table in tables:
            if table not in tables_to_generate:
                tables_to_generate.append(table)
        
        for table_name in tables_to_generate:
            if table_name not in self.schema_info:
                print(f"  ⚠ Skipping {table_name} (not inspected)")
                continue
            
            table_data = []
            pk_column = self.schema_info[table_name]['primary_keys'][0]
            
            for i in range(1, self.records_per_table + 1):
                record = self._generate_record_for_table(table_name, i)
                table_data.append(record)
                
                # Register the ID for foreign key references
                if pk_column in record:
                    self.id_registry[table_name].append(record[pk_column])
            
            self.synthetic_data[table_name] = table_data
            print(f"  ✓ {table_name}: {len(table_data)} records generated")
        
        return self.synthetic_data
    
    def save_to_json(self, output_file: str = "synthetic_data.json") -> None:
        """
        Save synthetic data to a JSON file.
        
        Args:
            output_file: Path to output JSON file
        """
        print(f"\n💾 Saving synthetic data to {output_file}...")
        
        output = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'records_per_table': self.records_per_table,
                'tables': list(self.synthetic_data.keys()),
                'total_records': sum(len(records) for records in self.synthetic_data.values())
            },
            'data': self.synthetic_data
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, default=str)
        
        print(f"  ✓ Saved {output['metadata']['total_records']} records to {output_file}")
    
    def run(self, tables: List[str], output_file: str = "synthetic_data.json") -> None:
        """
        Run the complete synthetic data generation pipeline.
        
        Args:
            tables: List of table names to process
            output_file: Path to output JSON file
        """
        print("=" * 60)
        print("🚀 Synthetic Data Generator for dvdrental Database")
        print("=" * 60)
        
        # Step 1: Inspect schema (no data export)
        self.inspect_schema(tables)
        
        # Step 2: Generate synthetic data
        self.generate_fake_data(tables)
        
        # Step 3: Save to JSON
        self.save_to_json(output_file)
        
        print("\n✅ Synthetic data generation complete!")


def main():
    """Main entry point for the script."""
    # Database connection
    CONNECTION_STRING = "postgresql://postgres@localhost/dvdrental"
    
    # Tables to generate data for
    TABLES = ['customer', 'film', 'address', 'rental']
    
    # Number of records per table
    RECORDS_PER_TABLE = 50
    
    # Output file
    OUTPUT_FILE = "synthetic_data.json"
    
    # Create generator and run
    generator = SyntheticDataGenerator(
        connection_string=CONNECTION_STRING,
        records_per_table=RECORDS_PER_TABLE
    )
    
    generator.run(tables=TABLES, output_file=OUTPUT_FILE)


if __name__ == "__main__":
    main()
