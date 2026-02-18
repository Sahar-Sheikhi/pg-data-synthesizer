# Database configuration
DB_NAME = "dvdrental"
DB_USER = "postgres"
DB_PASS = "mypassword"
DB_HOST = "127.0.0.1"  # Use 127.0.0.1 instead of localhost for TCP/IP
DB_PORT = "5432"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Synthesis Settings
ROW_COUNT = 1000  # How many fake records to generate per table