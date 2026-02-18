# PostgreSQL Synthetic Data Engine 🛡️

A universal, metadata-driven Python engine designed to generate **statistically consistent synthetic data** from any Postgres database without ever reading a single row of real-world data.

<br>

---

<br>

## 🚀 The Core Philosophy: Zero-Knowledge

Developing locally usually requires a choice: use empty databases or risk privacy by exporting real PII (Personally Identifiable Information). This project eliminates that choice by using a **Metadata-First** approach—learning the "blueprint" of the database without ever opening the "vault."

<br>

---

<br>

## ⚖️ Compliance & Use Cases

This framework is specifically engineered for environments governed by strict data sovereignty laws. It is ideal for:

* **GDPR Compliance**: Generates data that is mathematically decoupled from real individuals, exercising "Privacy by Design."
* **ISO/IEC 27001**: Supports Annex A.14 (System acquisition, development, and maintenance) by ensuring production data is never used in test environments.
* **SOC2 Audits**: Provides an immutable `audit_log.txt` to prove to auditors that PII was never accessed during the development lifecycle.
* **Safe Outsourcing**: Allows third-party developers to work on realistic data structures without seeing actual customer information.

<br>

---

<br>

## 🏗️ Technical Architecture

The project is built on a modular four-layer security model:

| Layer | Component | Responsibility |
| :--- | :--- | :--- |
| **1. The Firewall** | `db_inspector.py` | Queries the *Information Schema* only. No `SELECT *` allowed. |
| **2. The Engine** | `data_generator.py` | Dynamic type-mapping. Maps SQL types to `Faker` providers. |
| **3. The Black Box** | `audit_log.txt` | A real-time trace of all SQL commands as proof of non-access. |
| **4. The Auditor** | `validator.py` | Performs a "Negative Join" to mathematically prove 0% leakage. |

<br>

---

<br>

## 🛡️ Data Privacy & Inspection Logic

This project ensures total data isolation by creating a hard boundary between structure and content.

* **What it READS**: Table names, column names, data types, and relational constraints (PK/FK).
* **What it NEVER Reads**: Row values, PII/PHI, or physical storage indexes.
* **Logical vs. Physical**: The script ignores physical **Indexes** (B-Trees) because they are optimizations for existing data. Instead, it recreates the **Logical Constraints**, allowing the target database to build its own fresh indexes.

<br>

---

<br>

## 📊 Verification Results

Running the suite against a standard Postgres sample database yields the following security metrics:

* **PII Overlap (Leakage)**: **0.0%**
* **Constraint Integrity**: Foreign keys are maintained via an in-memory `SharedRegistry`.
* **Audit Trace**: 100% Metadata-only queries (captured in `audit_log.txt`).

<br>

---

<br>

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

<br>

---

<br>

## 📜 License
MIT