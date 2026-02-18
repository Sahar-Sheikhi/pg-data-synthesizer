"""
Data Generator - The "Painter" that creates synthetic data.

This module generates fake data using the Faker library based on schema metadata.
It maintains referential integrity using an in-memory ID registry.
"""

from typing import Dict, List, Any
from datetime import datetime
from faker import Faker


class DataGenerator:
    """
    Generates synthetic data while maintaining referential integrity.
    Uses Faker to create realistic fake data based on column names and types.
    """
    
    def __init__(self, schema_metadata: Dict[str, Any], records_per_table: int = 50):
        """
        Initialize the data generator.
        
        Args:
            schema_metadata: Schema metadata from DatabaseInspector
            records_per_table: Number of records to generate per table
        """
        self.schema_metadata = schema_metadata
        self.records_per_table = records_per_table
        self.faker = Faker()
        
        # In-Memory Registry to track generated IDs for referential integrity
        self.id_registry: Dict[str, List[int]] = {}
        
        # Generated synthetic data
        self.synthetic_data: Dict[str, List[Dict]] = {}
    
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
        # Handle nullable columns (10% chance of NULL)
        if is_nullable and self.faker.boolean(chance_of_getting_true=10):
            return None
        
        column_lower = column_name.lower()
        
        # String/VARCHAR types
        if 'VARCHAR' in column_type or 'TEXT' in column_type or 'CHARACTER VARYING' in column_type:
            if 'email' in column_lower:
                return self.faker.email()
            elif 'first_name' in column_lower:
                return self.faker.first_name()
            elif 'last_name' in column_lower:
                return self.faker.last_name()
            elif column_lower == 'address':
                return self.faker.street_address()
            elif 'address2' in column_lower:
                return self.faker.secondary_address() if self.faker.boolean(chance_of_getting_true=30) else None
            elif 'district' in column_lower:
                return self.faker.city()
            elif 'phone' in column_lower:
                return self.faker.phone_number()[:20]
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
                # Extract max length if specified
                if '(' in column_type:
                    try:
                        max_len = int(column_type.split('(')[1].split(')')[0])
                        return self.faker.text(max_nb_chars=min(max_len, 50))[:max_len]
                    except:
                        pass
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
    
    def _generate_record(self, table_name: str, record_id: int) -> Dict[str, Any]:
        """
        Generate a single synthetic record for a table.
        
        Args:
            table_name: Name of the table
            record_id: ID for this record
            
        Returns:
            Dictionary representing a synthetic record
        """
        record = {}
        table_metadata = self.schema_metadata[table_name]
        
        for column in table_metadata['columns']:
            col_name = column['name']
            col_type = str(column['type'])
            is_nullable = column['nullable']
            
            # Handle primary key
            if col_name in table_metadata['primary_keys']:
                record[col_name] = record_id
                continue
            
            # Handle foreign keys
            is_foreign_key = False
            for fk in table_metadata['foreign_keys']:
                if col_name in fk['constrained_columns']:
                    is_foreign_key = True
                    referenced_table = fk['referred_table']
                    
                    # Get a valid ID from the registry
                    if referenced_table in self.id_registry and self.id_registry[referenced_table]:
                        record[col_name] = self.faker.random_element(self.id_registry[referenced_table])
                    else:
                        # Fallback: generate a small integer
                        record[col_name] = self.faker.random_int(min=1, max=10)
                    break
            
            if not is_foreign_key:
                # Generate synthetic value based on column type and name
                record[col_name] = self._generate_value_for_column(col_name, col_type, is_nullable)
        
        return record
    
    def generate_data(self, tables: List[str]) -> Dict[str, List[Dict]]:
        """
        Generate synthetic data for specified tables.
        
        Args:
            tables: List of table names to generate data for
            
        Returns:
            Dictionary mapping table names to lists of synthetic records
        """
        print(f"\n🎨 Generating {self.records_per_table} synthetic records per table...")
        
        # Generation order matters for referential integrity
        # Generate in dependency order
        ordered_tables = self._order_tables_by_dependencies(tables)
        
        for table_name in ordered_tables:
            if table_name not in self.schema_metadata:
                print(f"  ⚠ Skipping {table_name} (no metadata)")
                continue
            
            table_data = []
            pk_columns = self.schema_metadata[table_name]['primary_keys']
            
            if not pk_columns:
                print(f"  ⚠ Skipping {table_name} (no primary key)")
                continue
            
            pk_column = pk_columns[0]  # Use first primary key
            
            # Initialize ID registry for this table
            if table_name not in self.id_registry:
                self.id_registry[table_name] = []
            
            for i in range(1, self.records_per_table + 1):
                record = self._generate_record(table_name, i)
                table_data.append(record)
                
                # Register the ID for foreign key references
                if pk_column in record:
                    self.id_registry[table_name].append(record[pk_column])
            
            self.synthetic_data[table_name] = table_data
            print(f"  ✓ {table_name}: {len(table_data)} records generated")
        
        return self.synthetic_data
    
    def _order_tables_by_dependencies(self, tables: List[str]) -> List[str]:
        """
        Order tables based on foreign key dependencies.
        Tables with no dependencies come first.
        
        Args:
            tables: List of table names
            
        Returns:
            Ordered list of table names
        """
        # Simple ordering: address -> customer -> film -> rental
        # This works for the dvdrental schema
        priority_order = ['address', 'customer', 'film', 'rental']
        
        ordered = []
        for table in priority_order:
            if table in tables:
                ordered.append(table)
        
        # Add any remaining tables
        for table in tables:
            if table not in ordered:
                ordered.append(table)
        
        return ordered
    
    def get_data(self) -> Dict[str, List[Dict]]:
        """
        Get the generated synthetic data.
        
        Returns:
            Dictionary of synthetic data
        """
        return self.synthetic_data
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the generated data.
        
        Returns:
            Dictionary containing generation statistics
        """
        total_records = sum(len(records) for records in self.synthetic_data.values())
        
        return {
            'total_tables': len(self.synthetic_data),
            'total_records': total_records,
            'records_per_table': {
                table: len(records) 
                for table, records in self.synthetic_data.items()
            }
        }
