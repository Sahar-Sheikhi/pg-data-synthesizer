# PG Synthetic Foundry

A lightweight Python framework to generate **statistically consistent synthetic data** from a Postgres database without exporting any real-world records.

## 🚀 The Challenge
Developing locally often requires realistic data, but moving production data to local machines creates security risks and violates privacy standards (GDPR/SOC2).

## 💡 The Solution
This project uses **Metadata Reflection** to learn the structure of a database (specifically tested on the `dvdrental` sample) and recreates a "Shadow Database" using:
* **SQLAlchemy** to inspect schemas without querying rows.
* **Faker** to generate realistic, non-identifiable information.
* **In-Memory Registry** to maintain Foreign Key integrity across tables.

## 🛠️ Project Structure
- `db_inspector.py`: Connects to Postgres to map tables and constraints.
- `data_generator.py`: Contains the logic for creating fake entities (Customers, Rentals).
- `main.py`: The orchestrator that builds the synthetic foundation.

## 🚦 Getting Started
1. Restore the `dvdrental.tar` to your local Postgres.
2. Update `config.py` with your credentials.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt