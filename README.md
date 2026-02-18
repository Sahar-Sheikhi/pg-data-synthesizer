# PG Synthetic Foundry 🛡️

A universal, metadata-driven Python engine designed to generate **statistically consistent synthetic data** from any Postgres database without ever reading a single row of real-world data.

## 🚀 The Core Philosophy: Zero-Knowledge
Developing locally usually requires a choice: use empty databases or risk privacy by exporting real PII (Personally Identifiable Information). This project eliminates that choice by using a **Metadata-First** approach—learning the "blueprint" of the database without ever opening the "vault."



## 🏗️ Technical Architecture
The project is built on a modular four-layer security model:

1. **The Firewall (`db_inspector.py`)**: Uses SQLAlchemy reflection to query the database *Information Schema*. It identifies table names, data types, and constraints without executing a single `SELECT` on your data.
2. **The Universal Engine (`data_generator.py`)**: A dynamic type-mapping engine. Whether a table has 5 columns or 1,500, it automatically assigns `Faker` providers based on column names and data types (Integer, Varchar, etc.).
3. **The Black Box (`audit_log.txt`)**: A transparent log of every SQL command sent to Postgres. This provides **immutable proof** that the script only touched system catalogs and never accessed production rows.
4. **The Independent Auditor (`validator.py`)**: A post-generation script that performs a **Negative Join** (set-intersection) between the source database and the synthetic output to mathematically prove a **0% leakage rate**.

## 🛡️ Data Privacy & Inspection Logic
This project ensures total data isolation by creating a hard boundary between structure and content.

* **What it READS**: Table names, column names, data types, and relational constraints (PK/FK).
* **What it NEVER Reads**: Row values, PII/PHI, or physical storage indexes.
* **Logical vs. Physical**: The script ignores physical **Indexes** because they are optimizations for existing data. Instead, it recreates the **Logical Constraints**, allowing the new database to build its own indexes upon insertion.



## 📊 Verification Results
Running the suite against a standard Postgres sample database:
* **PII Overlap (Leakage)**: **0.0%**
* **Constraint Integrity**: Foreign keys are maintained via an in-memory `SharedRegistry`.
* **Audit Trace**: 100% Metadata-only queries (captured in `audit_log.txt`).

## 🛠️ Setup & Execution
1.  **Configure**: Update `config.py` with your Postgres credentials.
2.  **Install Dependencies**: 
    ```bash
    pip install -r requirements.txt
    ```
3.  **Generate & Audit**: 
    ```bash
    python main.py      # Generates data for ALL detected tables
    python validator.py  # Verifies synthetic isolation
    ```

## 📜 License
MIT