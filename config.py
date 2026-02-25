# Database configuration
DB_NAME = "dvdrental"
DB_USER = "postgres"
DB_PASS = "mypassword"
DB_HOST = "127.0.0.1"  # TCP/IP connection to PostgreSQL
DB_PORT = "5432"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Synthesis Settings
# Note: Row counts are automatically read from source database to maintain
# realistic cardinality and referential integrity. To scale up/down, modify
# the source query in DBInspector.get_table_row_count() method.