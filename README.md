# PostgreSQL Synthetic Data Generator

Metadata-driven synthetic data generation for PostgreSQL databases. Generates type-safe synthetic data while maintaining referential integrity and matching source database cardinality.

## Overview

This tool generates synthetic data by analyzing database schema metadata without reading actual data. It preserves foreign key relationships, primary key constraints, and table dependencies while ensuring zero data leakage.

## Features

- **Metadata-only access**: Reads schema, constraints, and row counts only
- **Type-safe generation**: Supports Integer, String, Boolean, DateTime, Date, Numeric, Float, Enum, Array
- **Referential integrity**: Maintains foreign key relationships and composite keys
- **Dynamic cardinality**: Matches source database row counts per table
- **Memory-optimized**: O(1) registry storage per table for large-scale databases
- **Audit logging**: All SQL queries logged to `audit_log.txt` for compliance verification

## Architecture

| Component | Purpose |
|-----------|---------|
| `db_inspector.py` | Schema introspection via information_schema queries |
| `data_generator.py` | Type-based synthetic data generation with constraint handling |
| `main.py` | Orchestrates dependency-ordered data generation |
| `loader.py` | Loads synthetic data into target PostgreSQL database |
| `validator.py` | Validates PII leakage and referential integrity |
| `comprehensive_validator.py` | Analyzes cardinality, distribution, NULLs, relationships, temporal patterns |

## Requirements

- Python 3.8+
- PostgreSQL database
- Dependencies: SQLAlchemy, Pandas, Faker

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Edit `config.py` with database credentials:

```python
DB_NAME = "your_database"
DB_USER = "postgres"
DB_PASS = "password"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
```

## Usage

### Generate Synthetic Data

```bash
python main.py
```

Outputs: `synthetic_foundation.json` with synthetic records matching source cardinality.

### Load into Target Database

```bash
python loader.py
```

Creates `synthetic_dev_db` database with synthetic data maintaining schema and constraints.

### Validate Data Quality

```bash
python validator.py                    # Basic integrity check
python comprehensive_validator.py      # Full quality analysis
python verify_samples.py               # Side-by-side comparison
```

## Technical Details

### Data Generation Process

1. **Schema Analysis**: Extracts table names, columns, types, PKs, FKs, enum values
2. **Dependency Ordering**: Sorts tables by foreign key count (parents before children)
3. **Type Mapping**: Generates values based on SQLAlchemy type inspection
4. **Constraint Handling**: 
   - Primary keys: Sequential generation per table
   - Foreign keys: Random selection from parent PK range
   - Composite keys: Unique tuple tracking
   - Unique constraints: Value deduplication for manager/email columns

### Memory Optimization

Registry stores PK ranges `{min, max}` instead of full value lists, reducing memory from O(n) to O(1) per table.

### Supported Data Types

- Numeric: Integer, Numeric, Float
- Text: String (with length constraints), Enum
- Temporal: Date, DateTime
- Boolean: True/False
- Complex: Array (recursive generation)

## Validation Metrics

The comprehensive validator analyzes:

1. **Cardinality**: Row count matching per table
2. **Distribution**: Value skew and uniqueness analysis
3. **NULL Percentage**: Comparison with source (currently 0%)
4. **Relationships**: Foreign key cardinality ratios
5. **Temporal Distribution**: Date/time range analysis
6. **Referential Integrity**: Orphaned FK detection

## Database Compatibility

Primary support: PostgreSQL 9.6+

PostgreSQL-specific features:
- Enum type extraction via `pg_enum` catalog
- Array type support

Other databases: Schema introspection is database-agnostic via SQLAlchemy; enum handling will use fallback values.

## Limitations

- NULL generation: Currently disabled (all columns populated)
- Composite PK limit: Tables with >100 unique combinations may experience collisions
- Text generation: Generic lorem ipsum (not domain-specific)

## License

This project is provided as-is for database development and testing purposes.

## 📜 License
MIT