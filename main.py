from temporal_db import TemporalDB  # ייבוא המחלקה מהקובץ השני
from datetime import datetime
import pandas as pd

if __name__ == "__main__":
    sys = TemporalDB()

    # וודא שהקובץ אקסל באותה תיקייה
    file_name = "project_db_2025.xlsx"
    sys.load_data(file_name)

    if sys.db.empty:
        print("Database is empty or failed to load. Exiting.")
        exit()

    while True:
        print("\n" + "=" * 40)
        print("   TEMPORAL DB - INTERACTIVE MENU")
        print("=" * 40)
        print("1. Retrieve (שאילתת אחזור)")
        print("2. Delete (מחיקה לוגית)")
        print("3. Update (עדכון לוגי)")
        print("4. Exit (יציאה)")

        choice = input("\nSelect option (1-4): ")

        if choice == '1':
            print("\n--- NEW QUERY ---")
            p_first = input("Patient First Name: ")
            p_last = input("Patient Last Name: ")
            code = input("LOINC Code (e.g. 11218-5): ")
            v_time_str = input("Valid Time (DD/MM/YYYY HH:MM) [Enter for Today]: ")
            t_time_str = input("Perspective Time (DD/MM/YYYY HH:MM) [Enter for Now]: ")

            try:
                v_time = pd.to_datetime(v_time_str, dayfirst=True) if v_time_str.strip() else datetime.now()
                t_time = pd.to_datetime(t_time_str, dayfirst=True) if t_time_str.strip() else datetime.now()
                print(sys.query_retrieve(p_first, p_last, code, v_time, t_time))
            except Exception as e:
                print(f"Error parsing date: {e}")

        elif choice == '2':
            print("\n--- DELETE RECORD ---")
            p_first = input("Patient First Name: ")
            p_last = input("Patient Last Name: ")
            code = input("LOINC Code: ")
            v_time_str = input("Time of measurement to delete (DD/MM/YYYY HH:MM): ")
            try:
                v_time = pd.to_datetime(v_time_str, dayfirst=True)
                sys.operation_delete(p_first, p_last, code, v_time)
            except Exception as e:
                print(f"Error: {e}")

        elif choice == '3':
            print("\n--- UPDATE RECORD ---")
            p_first = input("Patient First Name: ")
            p_last = input("Patient Last Name: ")
            code = input("LOINC Code: ")
            v_time_str = input("Time of original measurement (DD/MM/YYYY HH:MM): ")
            new_val = input("Enter NEW Value: ")
            try:
                v_time = pd.to_datetime(v_time_str, dayfirst=True)
                sys.operation_update(p_first, p_last, code, v_time, new_val)
            except Exception as e:
                print(f"Error: {e}")

        elif choice == '4':
            print("Goodbye!")
            break
