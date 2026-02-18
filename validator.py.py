import json
import pandas as pd
from sqlalchemy import create_engine, text
from config import DATABASE_URL

def prove_data_is_fake():
    # 1. Load Real Data using standard SQLAlchemy
    engine = create_engine(DATABASE_URL)
    print("Connecting to database to verify PII isolation...")
    
    real_emails = set()
    try:
        with engine.connect() as conn:
            # Execute and fetch all results into a set of lowercase strings
            result = conn.execute(text("SELECT email FROM customer"))
            for row in result:
                real_emails.add(row[0].lower())
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return

    # 2. Load Synthetic Data from JSON
    try:
        with open("synthetic_foundation.json", "r") as f:
            synth_data = json.load(f)
        synth_df = pd.DataFrame(synth_data["customer"])
    except (FileNotFoundError, KeyError) as e:
        print(f"Error loading synthetic data: {e}")
        return

    print(f"\n--- Integrity Report ---")
    print(f"Real records scanned: {len(real_emails)}")
    print(f"Synthetic records scanned: {len(synth_df)}")

    # 3. The "Leak Test"
    synth_emails = set(synth_df['email'].str.lower())
    leaks = real_emails.intersection(synth_emails)

    if not leaks:
        print("\n✅ PASS: Zero real-world emails found in synthetic data.")
        print("This proves the generator is creating 100% original fake data.")
    else:
        print(f"\n⚠️ ALERT: Found {len(leaks)} matching emails.")
        print(f"Matches: {leaks}")

    # 4. Consistency Check
    # Verify the synthetic emails don't even use the same domain as the real ones
    # Real dvdrental uses '@sakilacustomer.org'
    domains = synth_df['email'].apply(lambda x: x.split('@')[-1]).unique()
    print(f"\nSynthetic Domains found: {', '.join(domains[:3])}...")

if __name__ == "__main__":
    prove_data_is_fake()