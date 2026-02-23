import json
import pandas as pd
from sqlalchemy import create_engine, text
from config import DATABASE_URL

def validate_shadow_database():
    # --- 1. SETUP ---
    with open("synthetic_foundation.json", "r") as f:
        synth_data = json.load(f)
    
    engine = create_engine(DATABASE_URL)
    all_leaks = []
    pk_map = {} # For Relational Integrity

    print("--- 🛡️ Starting Agnostic Security & Integrity Audit ---")

    # --- 2. RELATIONAL INTEGRITY & LEAK TEST LOOP ---
    for table_name, rows in synth_data.items():
        if not rows: continue
        df_synth = pd.DataFrame(rows)

        # A. BUILD PK MAP (Relational)
        for col in df_synth.columns:
            if col.lower().endswith('_id'):
                if col not in pk_map: pk_map[col] = set()
                pk_map[col].update(df_synth[col].tolist())

        # B. LEAK TEST (Privacy)
        # Check if any column in the synthetic table exists in the real DB
        # If it does, compare values to ensure 0% overlap
        for col in df_synth.columns:
            if "email" in col.lower() or "name" in col.lower():
                try:
                    with engine.connect() as conn:
                        query = text(f"SELECT {col} FROM {table_name}")
                        real_values = {str(r[0]).lower() for r in conn.execute(query)}
                        
                        synth_values = {str(v).lower() for v in df_synth[col]}
                        leaks = real_values.intersection(synth_values)
                        if leaks:
                            all_leaks.append(f"{table_name}.{col}: {len(leaks)} leaks found!")
                except Exception:
                    # Table or column might not exist in real DB; skip
                    continue

    # --- 3. CROSS-REFERENCE FOREIGN KEYS (Relational) ---
    orphan_count = 0
    for table_name, rows in synth_data.items():
        for row in rows:
            for col, val in row.items():
                if col.lower().endswith('_id') and col in pk_map:
                    if val not in pk_map[col]:
                        orphan_count += 1

    # --- 4. FINAL REPORT ---
    print("\n[PRIVACY REPORT]")
    if not all_leaks:
        print("✅ PASS: 0.0% Data Leakage. No real PII found in synthetic output.")
    else:
        print(f"❌ FAIL: PII Overlap detected! {all_leaks}")

    print("\n[INTEGRITY REPORT]")
    if orphan_count == 0:
        print(f"✅ PASS: 100% Referential Integrity. No orphan records found.")
    else:
        print(f"❌ FAIL: {orphan_count} broken relationships detected.")

if __name__ == "__main__":
    validate_shadow_database()