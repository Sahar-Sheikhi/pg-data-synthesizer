import random
from faker import Faker

class DataGenerator:
    def __init__(self, inspector):
        self.inspector = inspector
        self.fake = Faker()
        self.registry = {}  # Primary key registry for foreign key references
        self.counters = {}  # Sequential primary key generators

    def generate_data(self, num_rows=10):
        all_data = {}
        tables = self.inspector.get_all_tables()
        
        # Tables should be processed in dependency order to ensure referential integrity
        for table in tables:
            print(f"Synthesizing {table}...")
            all_data[table] = []
            
            # Retrieve table metadata
            pks = self.inspector.get_primary_keys(table)
            fks_map = self.inspector.get_real_foreign_keys(table)
            columns = self.inspector.get_columns(table)

            for _ in range(num_rows):
                row = {}
                for col_meta in columns:
                    col_name = col_meta['name']
                    
                    # Primary key: Generate sequential value
                    if col_name in pks:
                        val = self.counters.get(col_name, 0) + 1
                        self.counters[col_name] = val
                        row[col_name] = val
                        
                        # Register primary key for foreign key assignment
                        if table not in self.registry: self.registry[table] = []
                        self.registry[table].append(val)

                    # Foreign key: Reference existing primary key
                    elif col_name in fks_map:
                        referred_table = fks_map[col_name]
                        # Select random primary key from referenced table
                        if referred_table in self.registry and self.registry[referred_table]:
                            row[col_name] = random.choice(self.registry[referred_table])
                        else:
                            row[col_name] = None  # No available reference

                    # Regular column: Generate synthetic data
                    else:
                        row[col_name] = self.generate_faker_value(col_name, col_meta['type'])
                
                all_data[table].append(row)
        
        return all_data

    def generate_faker_value(self, name, col_type):
        # Map column names and types to appropriate Faker methods
        if "email" in name.lower(): return self.fake.email()
        return self.fake.word()