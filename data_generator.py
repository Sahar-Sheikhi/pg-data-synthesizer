"""
Synthetic Data Generator

Generates type-safe synthetic data for database tables while maintaining referential
integrity through primary key tracking and foreign key constraint handling.
"""

import random
from faker import Faker
from sqlalchemy import Integer, String, Boolean, DateTime, Numeric, Float, Date, Enum, ARRAY
from sqlalchemy.dialects.postgresql import ENUM as PG_ENUM


class DataGenerator:
    """
    Generates synthetic data with constraint-aware value generation.
    
    Attributes:
        inspector: DBInspector instance for schema introspection
        fake: Faker instance for generating realistic data
        registry: Primary key ranges (min/max) per table for FK references
        counters: Table-specific primary key counters to prevent collisions
    """
    
    def __init__(self, inspector):
        self.inspector = inspector
        self.fake = Faker()
        self.registry = {}
        self.counters = {}

    def generate_table_data(self, table_name, table_schema, count):
        """
        Generate synthetic rows for a table respecting constraints.
        
        Args:
            table_name: Name of the table to generate data for
            table_schema: Dictionary mapping column names to SQLAlchemy types
            count: Number of rows to generate
            
        Returns:
            List of dictionaries representing table rows
        """
        pks = self.inspector.get_primary_keys(table_name)
        fks_map = self.inspector.get_real_foreign_keys(table_name)
        unique_trackers = {}
        composite_pk_tracker = set()  # Track composite PK combinations
        
        table_rows = []
        for _ in range(count):
            row = {}
            for col_name, col_type in table_schema.items():
                # If column is both PK and FK (composite key), treat as FK
                if col_name in pks and col_name not in fks_map:
                    counter_key = f"{table_name}.{col_name}"
                    val = self.counters.get(counter_key, 0) + 1
                    self.counters[counter_key] = val
                    row[col_name] = val
                    if table_name not in self.registry:
                        self.registry[table_name] = {'min': val, 'max': val}
                    else:
                        self.registry[table_name]['max'] = val

                elif col_name in fks_map:
                    referred_table = fks_map[col_name]
                    pk_range = self.registry.get(referred_table, {'min': 1, 'max': 1})
                    needs_unique = 'manager' in col_name.lower()
                    
                    if needs_unique:
                        fk_tracker_key = f"{table_name}.{col_name}"
                        if fk_tracker_key not in unique_trackers:
                            unique_trackers[fk_tracker_key] = set()
                        
                        attempts = 0
                        while attempts < 50:
                            value = random.randint(pk_range['min'], pk_range['max'])
                            if value not in unique_trackers[fk_tracker_key]:
                                unique_trackers[fk_tracker_key].add(value)
                                row[col_name] = value
                                break
                            attempts += 1
                        else:
                            row[col_name] = random.randint(pk_range['min'], pk_range['max'])
                    else:
                        row[col_name] = random.randint(pk_range['min'], pk_range['max'])

                else:
                    value = self.generate_validated_value(col_name, col_type)
                    
                    if value is None:
                        enum_values = self.inspector.get_enum_values(table_name, col_name)
                        if enum_values:
                            value = random.choice(enum_values)
                        else:
                            value = f"{col_name}_{random.randint(1, 9999)}"
                    
                    if any(keyword in col_name.lower() for keyword in ['manager', 'email', 'username']):
                        if col_name not in unique_trackers:
                            unique_trackers[col_name] = set()
                        
                        attempts = 0
                        while value in unique_trackers[col_name] and attempts < 50:
                            value = self.generate_validated_value(col_name, col_type)
                            attempts += 1
                        
                        unique_trackers[col_name].add(value)
                    
                    row[col_name] = value
            
            # Handle composite PK uniqueness (when all PKs are also FKs)
            if len(pks) > 1 and all(pk in fks_map for pk in pks):
                pk_tuple = tuple(row[pk] for pk in sorted(pks))
                attempts = 0
                while pk_tuple in composite_pk_tracker and attempts < 100:
                    # Regenerate FK values for the composite key
                    for pk_col in pks:
                        referred_table = fks_map[pk_col]
                        pk_range = self.registry.get(referred_table, {'min': 1, 'max': 1})
                        row[pk_col] = random.randint(pk_range['min'], pk_range['max'])
                    pk_tuple = tuple(row[pk] for pk in sorted(pks))
                    attempts += 1
                composite_pk_tracker.add(pk_tuple)
            
            table_rows.append(row)
        return table_rows

    def generate_validated_value(self, name, col_type):
        """
        Generate type-safe values based on SQLAlchemy column types.
        
        Args:
            name: Column name (used for context)
            col_type: SQLAlchemy type object (Integer, String, Date, etc.)
            
        Returns:
            Generated value matching the expected Python type, or None for unhandled types
        """
        if isinstance(col_type, Integer):
            return random.randint(1, 1000)
            
        if isinstance(col_type, (Numeric, Float)):
            return round(random.uniform(1.0, 100.0), 2)
            
        if isinstance(col_type, Boolean):
            return random.choice([True, False])
        
        if isinstance(col_type, Date):
            return self.fake.date_this_year().isoformat()
            
        if isinstance(col_type, DateTime):
            return self.fake.date_time_this_year().isoformat()
        
        if isinstance(col_type, (Enum, PG_ENUM)) or (hasattr(col_type, 'enums') and col_type.enums):
            if hasattr(col_type, 'enums') and col_type.enums:
                return random.choice(col_type.enums)
            return None
        
        if isinstance(col_type, ARRAY):
            array_size = random.randint(0, 3)
            item_type = col_type.item_type if hasattr(col_type, 'item_type') else String
            return [self.generate_validated_value(name, item_type) for _ in range(array_size)]
            
        if isinstance(col_type, String):
            length = col_type.length if hasattr(col_type, 'length') and col_type.length else 50
            return self.fake.text(max_nb_chars=length)[:length].strip()

        return None