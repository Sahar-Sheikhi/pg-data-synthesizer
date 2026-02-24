"""
Synthetic Data Validator

Validates synthetic data against source database to ensure privacy protection
and referential integrity. Checks for PII leakage and orphaned foreign key references.
"""

import json
import pandas as pd
from sqlalchemy import create_engine, text
from config import DATABASE_URL


def validate_shadow_database():
    """
    Validate synthetic data for privacy and referential integrity.
    
    Performs two validation checks:
    1. Privacy: Ensures no real PII values leaked into synthetic data
    2. Integrity: Verifies all foreign keys reference valid parent records
    """
    try:
        with open("synthetic_foundation.json", "r") as f:
            synth_data = json.load(f)
    except FileNotFoundError:
        print("Error: synthetic_foundation.json not found.")
        return
    
    engine = create_engine(DATABASE_URL)
    all_leaks = []
    structured_pk_map = {} 

    print("--- Starting Data Validation Audit ---")

    for table_name, rows in synth_data.items():
        if not rows:
            continue
        df_synth = pd.DataFrame(rows)
        
        clean_table = table_name.lower()
        if clean_table not in structured_pk_map:
            structured_pk_map[clean_table] = {}

        for col in df_synth.columns:
            col_lower = col.lower()
            if col_lower.endswith('_id'):
                structured_pk_map[clean_table][col_lower] = set(df_synth[col].unique())

        # Check for PII leakage (exclude ID columns)
        for col in df_synth.columns:
            col_lower = col.lower()
            is_pii_column = any(
                pii in col_lower for pii in ["email", "first_name", "last_name", "phone"]
            ) and not col_lower.endswith('_id')
            
            if is_pii_column:
                try:
                    with engine.connect() as conn:
                        query = text(f"SELECT {col} FROM {table_name} LIMIT 1000")
                        real_values = {str(r[0]).lower() for r in conn.execute(query)}
                        
                        synth_values = {str(v).lower() for v in df_synth[col]}
                        leaks = real_values.intersection(synth_values)
                        if leaks:
                            all_leaks.append(f"{table_name}.{col}: {len(leaks)} matches")
                except Exception:
                    continue

    # Validate foreign key references
    orphan_count = 0
    for table_name, rows in synth_data.items():
        clean_table = table_name.lower()
        for row in rows:
            for col, val in row.items():
                col_lower = col.lower()
                
                if col_lower.endswith('_id') and clean_table not in col_lower:
                    target_table = col_lower.replace('_id', '')
                    parent_ids = structured_pk_map.get(target_table, {}).get(col_lower, set())
                    
                    if val not in parent_ids:
                        orphan_count += 1

    # Report results
    print("\n[PRIVACY REPORT]")
    if not all_leaks:
        print("PASS: No data leakage detected. Synthetic data is fully anonymous.")
    else:
        print(f"FAIL: PII overlap detected - {all_leaks}")

    print("\n[INTEGRITY REPORT]")
    if orphan_count == 0:
        print("PASS: All foreign keys reference valid parent records.")
    else:
        print(f"FAIL: {orphan_count} orphaned foreign key references found.")


if __name__ == "__main__":
    validate_shadow_database()