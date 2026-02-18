import random
from faker import Faker

class DataGenerator:
    def __init__(self):
        self.fake = Faker()
        # The Registry stores IDs we've generated so they can be reused as Foreign Keys
        self.registry = {}
        self.counters = {}

    def get_next_id(self, col_name):
        """Sequential ID generator for Primary Keys."""
        if col_name not in self.counters:
            self.counters[col_name] = 1
        else:
            self.counters[col_name] += 1
        
        new_id = self.counters[col_name]
        
        # Store in registry so other tables can reference these IDs
        if col_name not in self.registry:
            self.registry[col_name] = []
        self.registry[col_name].append(new_id)
        
        return new_id

    def generate_generic_row(self, table_schema):
        """
        Universal generator: Works for ANY number of columns.
        Uses column names and types to decide what data to 'fake'.
        """
        row = {}
        for col_name, col_type in table_schema.items():
            col_name_lower = col_name.lower()
            col_type_str = str(col_type).upper()

            # 1. PRIMARY KEYS & FOREIGN KEYS
            # If it's an ID we've seen before but NOT the primary table, treat as FK
            if "id" in col_name_lower:
                if col_name_lower in self.registry and random.random() > 0.1:
                    row[col_name] = random.choice(self.registry[col_name_lower])
                else:
                    row[col_name] = self.get_next_id(col_name_lower)

            # 2. SMART HINTS (Based on Column Names)
            elif "email" in col_name_lower:
                row[col_name] = self.fake.email()
            elif "first_name" in col_name_lower:
                row[col_name] = self.fake.first_name().upper()
            elif "last_name" in col_name_lower:
                row[col_name] = self.fake.last_name().upper()
            elif "name" in col_name_lower:
                row[col_name] = self.fake.name()
            elif "address" in col_name_lower:
                row[col_name] = self.fake.address()
            elif "phone" in col_name_lower:
                row[col_name] = self.fake.phone_number()

            # 3. TYPE-BASED FALLBACKS (The 'Limitless' Logic)
            elif "VARCHAR" in col_type_str or "TEXT" in col_type_str:
                row[col_name] = self.fake.word()
            elif "INT" in col_type_str:
                row[col_name] = random.randint(1, 100)
            elif "BOOL" in col_type_str:
                row[col_name] = random.choice([True, False])
            elif "DATE" in col_type_str or "TIME" in col_type_str:
                row[col_name] = self.fake.date_time_this_year()
            else:
                row[col_name] = None # Or a generic fake string

        return row

    def generate_table_data(self, table_name, table_schema, count):
        """Generates a list of rows for a specific table schema."""
        return [self.generate_generic_row(table_schema) for _ in range(count)]
