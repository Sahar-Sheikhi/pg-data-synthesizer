# PostgreSQL Data Synthesizer

A simple tool to generate synthetic data for PostgreSQL databases.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your database in `config.py`:
```python
DB_NAME = "dvdrental"
DB_USER = "postgres"
DB_PASS = "mypassword"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
```

3. Run the generator:
```bash
python main.py
```

## Features

- Inspects PostgreSQL database schema
- Generates synthetic data based on column types
- Respects foreign key relationships

## Requirements

- Python 3.x
- PostgreSQL database
