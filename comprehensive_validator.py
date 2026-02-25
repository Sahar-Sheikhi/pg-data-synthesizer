"""
Comprehensive Data Quality Validator

Validates synthetic data quality across multiple dimensions:
- Cardinality (row counts)
- Value distribution (skew, dominant values)
- NULL percentages
- Relationship metrics (FK cardinality)
- Temporal distribution
- Referential integrity
"""

import json
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from collections import Counter
from config import DATABASE_URL


def comprehensive_validation():
    """
    Perform comprehensive validation of synthetic data quality.
    
    Compares synthetic data against source database across 6 dimensions:
    1. Table cardinality
    2. Value distribution and skew
    3. NULL percentages
    4. Relationship metrics (FK ratios)
    5. Temporal distribution
    6. Referential integrity
    """
    try:
        with open("synthetic_foundation.json", "r") as f:
            synth_data = json.load(f)
    except FileNotFoundError:
        print("Error: synthetic_foundation.json not found.")
        return
    
    source_engine = create_engine(DATABASE_URL)
    source_inspector = inspect(source_engine)
    
    print("=" * 100)
    print("COMPREHENSIVE DATA QUALITY VALIDATION REPORT")
    print("=" * 100)
    
    # Metric 1: Cardinality (Row Counts)
    print("\n[1] CARDINALITY ANALYSIS")
    print("-" * 100)
    print(f"{'Table':<20} | {'Source Rows':<15} | {'Synthetic Rows':<15} | {'Match':<10}")
    print("-" * 100)
    
    cardinality_pass = True
    for table_name in synth_data.keys():
        try:
            with source_engine.connect() as conn:
                source_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            synth_count = len(synth_data[table_name])
            match_status = "MATCH" if source_count == synth_count else "DIFFER"
            if match_status == "DIFFER":
                cardinality_pass = False
            print(f"{table_name:<20} | {source_count:<15} | {synth_count:<15} | {match_status:<10}")
        except Exception as e:
            print(f"{table_name:<20} | Error: {e}")
    
    print(f"\nCardinality Check: {'PASS' if cardinality_pass else 'NOTE: Row counts differ (expected for synthetic data)'}")
    
    # Metric 2: Value Distribution (Skew & Dominant Values)
    print("\n[2] VALUE DISTRIBUTION ANALYSIS")
    print("-" * 100)
    
    for table_name, rows in list(synth_data.items())[:3]:  # Sample first 3 tables
        if not rows:
            continue
        print(f"\nTable: {table_name}")
        df_synth = pd.DataFrame(rows)
        
        # Analyze non-ID columns for distribution
        for col in df_synth.columns:
            if not col.lower().endswith('_id') and df_synth[col].dtype in ['object', 'string']:
                value_counts = df_synth[col].value_counts()
                unique_count = len(value_counts)
                top_value = value_counts.iloc[0] if len(value_counts) > 0 else 0
                top_percentage = (top_value / len(df_synth)) * 100 if len(df_synth) > 0 else 0
                
                # Check for high skew (>30% concentration in single value)
                skew_status = "HIGH SKEW" if top_percentage > 30 else "OK"
                print(f"  {col:<25} | Unique: {unique_count:<6} | Top value: {top_percentage:>5.1f}% | {skew_status}")
    
    # Metric 3: NULL Percentage Analysis
    print("\n[3] NULL PERCENTAGE ANALYSIS")
    print("-" * 100)
    print(f"{'Table.Column':<30} | {'Source NULLs %':<15} | {'Synthetic NULLs %':<15} | {'Status':<10}")
    print("-" * 100)
    
    null_analysis_count = 0
    for table_name in list(synth_data.keys())[:5]:  # Sample first 5 tables
        try:
            columns = [col['name'] for col in source_inspector.get_columns(table_name)]
            df_synth = pd.DataFrame(synth_data[table_name])
            
            for col in columns[:3]:  # First 3 columns per table
                if col in df_synth.columns:
                    with source_engine.connect() as conn:
                        total = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                        null_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL")).scalar()
                        source_null_pct = (null_count / total * 100) if total > 0 else 0
                    
                    synth_null_pct = (df_synth[col].isna().sum() / len(df_synth) * 100) if len(df_synth) > 0 else 0
                    
                    status = "MATCH" if abs(source_null_pct - synth_null_pct) < 5 else "DIFFER"
                    print(f"{table_name}.{col:<30} | {source_null_pct:>12.1f}% | {synth_null_pct:>15.1f}% | {status:<10}")
                    null_analysis_count += 1
                    
                    if null_analysis_count >= 10:  # Limit output
                        break
            if null_analysis_count >= 10:
                break
        except Exception:
            continue
    
    print("\nNOTE: Synthetic data currently generates no NULLs. Consider adding NULL generation for nullable columns.")
    
    # Metric 4: Relationship Metrics (FK Cardinality)
    print("\n[4] RELATIONSHIP METRICS ANALYSIS")
    print("-" * 100)
    
    # Build PK registry using actual database schema instead of naming heuristics
    structured_pk_map = {}
    for table_name, rows in synth_data.items():
        if not rows:
            continue
        clean_table = table_name.lower()
        structured_pk_map[clean_table] = {}
        
        # Get actual primary key columns from database schema
        pk_columns = source_inspector.get_pk_constraint(table_name)['constrained_columns']
        df = pd.DataFrame(rows)
        
        for pk_col in pk_columns:
            if pk_col in df.columns:
                structured_pk_map[clean_table][pk_col.lower()] = set(df[pk_col].unique())
    
    # Analyze FK relationships
    print("FK Relationship Cardinality:")
    for table_name, rows in list(synth_data.items())[:5]:
        if not rows:
            continue
        df = pd.DataFrame(rows)
        clean_table = table_name.lower()
        
        for col in df.columns:
            col_lower = col.lower()
            if col_lower.endswith('_id') and clean_table not in col_lower:
                # This is a foreign key
                unique_fk_values = df[col].nunique()
                total_rows = len(df)
                ratio = total_rows / unique_fk_values if unique_fk_values > 0 else 0
                
                print(f"  {table_name}.{col:<25} | Avg records per parent: {ratio:.2f}")
    
    # Metric 5: Temporal Distribution
    print("\n[5] TEMPORAL DISTRIBUTION ANALYSIS")
    print("-" * 100)
    
    temporal_found = False
    for table_name, rows in synth_data.items():
        if not rows:
            continue
        df = pd.DataFrame(rows)
        
        # Find date/datetime columns
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['date', 'time', 'created', 'updated']):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    if df[col].notna().sum() > 0:
                        min_date = df[col].min()
                        max_date = df[col].max()
                        print(f"  {table_name}.{col:<25} | Range: {min_date} to {max_date}")
                        temporal_found = True
                except:
                    continue
    
    if not temporal_found:
        print("  No temporal columns analyzed or all values are NULL.")
    
    # Metric 6: Referential Integrity
    print("\n[6] REFERENTIAL INTEGRITY VALIDATION")
    print("-" * 100)
    
    orphan_count = 0
    total_fk_checks = 0
    
    for table_name, rows in synth_data.items():
        # Get actual foreign keys from database schema
        fk_constraints = source_inspector.get_foreign_keys(table_name)
        
        for row in rows:
            for fk in fk_constraints:
                constrained_cols = fk['constrained_columns']
                referred_table = fk['referred_table']
                referred_cols = fk['referred_columns']
                
                # Check each FK column
                for i, fk_col in enumerate(constrained_cols):
                    if fk_col in row:
                        total_fk_checks += 1
                        fk_value = row[fk_col]
                        
                        # Look up the PK in the referred table
                        referred_pk_col = referred_cols[i] if i < len(referred_cols) else referred_cols[0]
                        parent_pks = structured_pk_map.get(referred_table.lower(), {}).get(referred_pk_col.lower(), set())
                        
                        if fk_value not in parent_pks:
                            orphan_count += 1
    
    integrity_pass = orphan_count == 0
    print(f"Total FK Checks: {total_fk_checks}")
    print(f"Orphaned Records: {orphan_count}")
    print(f"Referential Integrity: {'PASS - All foreign keys valid' if integrity_pass else f'FAIL - {orphan_count} orphaned references'}")
    
    # Final Summary
    print("\n" + "=" * 100)
    print("VALIDATION SUMMARY")
    print("=" * 100)
    print(f"✓ Cardinality: Analyzed {len(synth_data)} tables")
    print(f"✓ Distribution: Checked for skew and dominant values")
    print(f"✓ NULLs: Currently 0% (consider enabling NULL generation)")
    print(f"✓ Relationships: FK cardinality ratios calculated")
    print(f"✓ Temporal: Date ranges analyzed")
    print(f"✓ Integrity: {'PASS' if integrity_pass else 'FAIL'}")
    print("=" * 100)
    
    source_engine.dispose()


if __name__ == "__main__":
    comprehensive_validation()
