from temporal_db import TemporalDB  # ייבוא המחלקה מהקובץ השני
from datetime import datetime
import pandas as pd

if __name__ == "__main__":
    sys = TemporalDB()

    # וודא ששם הקובץ תואם לקובץ שלך שנמצא באותה תיקייה
    file_name = "project_db_2025.xlsx"
    sys.load_data(file_name)

    if sys.db.empty:
        print("Database is empty or failed to load. Exiting.")
        exit()

    while True:
        print("\n" + "=" * 40)
        print("   TEMPORAL DB - INTERACTIVE MENU")
        print("=" * 40)
        print("1. Retrieve (שאילתת אחזור רגילה)")
        print("2. Retrieve History (אחזור היסטוריה)")
        print("3. Delete (מחיקה לוגית)")
        print("4. Update (עדכון לוגי)")
        print("5. Exit (יציאה)")

        choice = input("\nSelect option (1-5): ")

        # --- אפשרות 1: אחזור רגיל ---
        if choice == '1':
            print("\n--- NEW QUERY (RETRIEVE) ---")
            p_first = input("Patient First Name: ")
            p_last = input("Patient Last Name: ")
            code = input("LOINC Code (e.g. 11218-5): ")
            v_time_str = input("Valid Time (DD/MM/YYYY HH:MM) [Enter for Today]: ")
            t_time_str = input("Perspective Time (DD/MM/YYYY HH:MM) [Enter for Now]: ")

            try:
                # ברירות מחדל: זמן תקף = היום, זמן טרנזקציה = עכשיו
                v_time = pd.to_datetime(v_time_str, dayfirst=True) if v_time_str.strip() else datetime.now()
                t_time = pd.to_datetime(t_time_str, dayfirst=True) if t_time_str.strip() else datetime.now()
                print(sys.query_retrieve(p_first, p_last, code, v_time, t_time))
            except Exception as e:
                print(f"Error parsing date: {e}")

        # --- אפשרות 2: אחזור היסטוריה (החדש) ---
        elif choice == '2':
            print("\n--- QUERY HISTORY ---")
            p_first = input("Patient First Name: ")
            p_last = input("Patient Last Name: ")
            code = input("LOINC Code (e.g. 14743-9): ")

            print("Define Time Range (Valid Time):")
            start_str = input("Start Date (DD/MM/YYYY HH:MM) [Enter for 01/01/1900]: ")
            end_str = input("End Date (DD/MM/YYYY HH:MM) [Enter for Now]: ")
            t_time_str = input("Perspective Time (DD/MM/YYYY HH:MM) [Enter for Now]: ")

            try:
                # המרת תאריכים עם ברירות מחדל
                start_time = pd.to_datetime(start_str, dayfirst=True) if start_str.strip() else pd.to_datetime(
                    "01/01/1900", dayfirst=True)
                end_time = pd.to_datetime(end_str, dayfirst=True) if end_str.strip() else datetime.now()
                t_time = pd.to_datetime(t_time_str, dayfirst=True) if t_time_str.strip() else datetime.now()

                # קריאה לפונקציה החדשה ב-TemporalDB
                # (אם יש שגיאה כאן, וודא שהוספת את query_history לקובץ temporal_db.py)
                print(sys.query_history(p_first, p_last, code, start_time, end_time, t_time))
            except Exception as e:
                print(f"Error: {e}")

        # --- אפשרות 3: מחיקה ---
        elif choice == '3':
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

        # --- אפשרות 4: עדכון ---
        elif choice == '4':
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

        # --- אפשרות 5: יציאה ---
        elif choice == '5':
            print("Goodbye!")
            break