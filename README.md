# PG Synthetic Foundry 🛡️

A lightweight Python framework to generate **statistically consistent synthetic data** from a Postgres database without exporting any real-world records.

## 🚀 The Challenge
Developing locally often requires realistic data, but moving production data to local machines creates security risks and violates privacy standards (GDPR/SOC2).

## 💡 The Solution: Zero-Knowledge Synthesis
This project uses **Metadata Reflection** to learn the "blueprint" of a database without ever reading the sensitive rows inside. We create a "Shadow Database" using:

* **The Firewall**: `SQLAlchemy` reflection to inspect schemas via system catalogs rather than table rows.
* **The Clean Room**: `Faker` to generate realistic, non-identifiable information from scratch.
* **The Registry**: An in-memory mapping system to maintain **Foreign Key integrity** across tables (e.g., linking synthetic rentals to synthetic customers).
* **The Proof**: An automated **Audit Logger** and **PII Validator** to ensure 0% data leakage.

## 🛠️ Project Structure
- `config.py`: Centralized database credentials and synthesis settings.
- `db_inspector.py`: Connects to Postgres to map metadata and logs an **Audit Trail** of all SQL commands.
- `data_generator.py`: The engine that creates fake entities while preserving relational logic.
- `main.py`: The orchestrator that builds the synthetic foundation.
- `validator.py`: A security script that mathematically proves the synthetic data has zero overlap with the real database.

## 🚦 Getting Started
1.  **Restore DB**: Restore the `dvdrental.tar` folder to your local Postgres instance.
2.  **Configure**: Update `config.py` with your local `postgreSQL` credentials.
3.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
4.  **Run & Verify**:
    ```bash
    python main.py      # Generate synthetic data + Audit log
    python validator.py  # Run the PII Leakage Audit
    ```

## 📊 Verification Results
The included `validator.py` confirms:
- **PII Overlap**: 0.0%
- **Audit Trace**: 100% Metadata-only queries (captured in `audit_log.txt`).
