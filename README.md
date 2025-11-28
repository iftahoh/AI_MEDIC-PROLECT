# Temporal Database Management System (Bi-Temporal)
**University Project - 2025**
## 📝 Author
  Name: Iftah Ohayon & Alexay Laikov

  Course: AI in Medicine / Database Management

  Year: 2025
  
## 📌 Project Overview
This project implements a **Bi-Temporal Database Management System** for medical records.
Unlike standard databases that store only current data, this system manages two time dimensions for every record:
1. **Valid Time:** When the medical event actually occurred (e.g., when the blood test was taken).
2. **Transaction Time:** When the data was recorded in the database.

This allows the system to support complex time-travel queries, such as: *"What did the doctor know about the patient's condition last Tuesday, before the lab corrected the results?"*

## 🚀 Features
* **Bi-Temporal Storage:** Supports both *Valid Time* and *Transaction Time*.
* **Insert-Only Architecture:** No physical deletion; data is preserved historically using timestamps.
* **Smart Data Loading:** Automatically detects CSV/Excel formats and handles variable column names.
* **Query Capabilities:**
    * **Retrieve:** Fetch data based on a specific perspective time.
    * **Update:** Logically update records while preserving history.
    * **Delete:** Logically delete records (soft delete) to maintain audit trails.

## 🛠️ Technology Stack
* **Language:** Python 3.x
* **Libraries:**
    * `pandas` (Data manipulation)
    * `openpyxl` (Excel file reading)
    * `datetime` (Time management)

## 📂 Project Structure
```text
├── project_db_2025.xlsx   # The initial dataset (Excel/CSV)
├── main.py                # Main source code (The TemporalDB class)
└── README.md              # Project documentation
```



