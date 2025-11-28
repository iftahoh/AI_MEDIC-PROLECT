import pandas as pd
from datetime import datetime
import os

print("--- Script Started ---")


class TemporalDB:
    def __init__(self):
        self.db = pd.DataFrame()
        # מילון מושגים להצגת שמות מלאים
        self.loinc_dictionary = {
            "12345": "Leukocytes [#/volume] in Blood by Automated count",
            "14743-9": "Glucose [Moles/volume] in Body fluid",
            "11218-5": "Anatomic pathology & Lab medicine"
        }

    def load_data(self, file_path):
        """ טעינת נתונים חכמה (תומכת ב-Excel ו-CSV ובשמות עמודות משתנים) """
        print(f"Attempting to load: {file_path}")

        if not os.path.exists(file_path):
            print(f"ERROR: File not found at {file_path}")
            return

        try:
            # שלב 1: זיהוי סוג הקובץ וטעינה
            if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
                df = pd.read_excel(file_path)
            else:
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                except:
                    df = pd.read_csv(file_path, encoding='cp1255')

            # שלב 2: ניקוי שמות העמודות
            df.columns = df.columns.str.strip().str.replace('"', '')

            # שלב 3: מיפוי שמות עמודות למבנה אחיד
            mapping_options = {
                'FirstName': ['First name', 'Firstname', 'Name', 'First'],
                'LastName': ['Last name', 'Lastname', 'Family Name', 'Last'],
                'LOINC': ['LOINC', 'LOINC-NUM', 'LOINC CODE', 'Code'],
                'Value': ['Value', 'Result'],
                'TransactionTime': ['Transaction time', 'TransactionTime', 'TT'],
                'ValidStartTime': ['Valid start time', 'ValidStartTime', 'Time'],
                'ValidStopTime': ['Valid stop time', 'ValidStopTime']
            }

            final_mapping = {}
            for target_col, options in mapping_options.items():
                for opt in options:
                    if opt in df.columns:
                        final_mapping[opt] = target_col
                        break

            df.rename(columns=final_mapping, inplace=True)

            # שלב 4: המרת תאריכים לפורמט זמן
            time_cols = ['TransactionTime', 'ValidStartTime', 'ValidStopTime']
            for col in time_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

            self.db = df
            print(f"SUCCESS: Loaded {len(self.db)} records.")
            print(f"Active Columns: {self.db.columns.tolist()}")

        except Exception as e:
            print(f"CRITICAL ERROR during loading: {e}")

    def get_loinc_desc(self, loinc_code):
        return self.loinc_dictionary.get(str(loinc_code), "Unknown Concept")

    # --- פונקציית הליבה: אחזור נתונים ---
    def query_retrieve(self, first_name, last_name, loinc, query_valid_time, query_transaction_time=None):
        if query_transaction_time is None:
            query_transaction_time = datetime.now()

        print(f"\n--- Query: {first_name} {last_name}, LOINC: {loinc} ---")

        # 1. סינון לפי שם וקוד בדיקה
        if self.db.empty:
            return "Database is empty."

        mask_patient = (self.db['FirstName'].astype(str).str.lower() == first_name.lower()) & \
                       (self.db['LastName'].astype(str).str.lower() == last_name.lower())
        mask_loinc = self.db['LOINC'].astype(str) == str(loinc)

        df = self.db[mask_patient & mask_loinc].copy()

        if df.empty:
            return "No records found for this patient/LOINC."

        # 2. סינון לפי זמן טרנזקציה (מה ידענו בנקודת המבט הזו?)
        # מתעלמים מכל מה שהוזן *אחרי* query_transaction_time
        df = df[df['TransactionTime'] <= query_transaction_time]

        if df.empty:
            return "No data existed at that Transaction Time."

        # 3. מציאת הנתון הנכון (התמודדות עם ריבוי גרסאות לאותה מדידה)
        # עבור כל זמן מדידה (ValidStartTime), אנו רוצים את השורה עם ה-TransactionTime הכי גבוה
        # (כלומר, הגרסה הכי עדכנית שהייתה קיימת בנקודת המבט שלנו)
        latest_indices = df.groupby('ValidStartTime')['TransactionTime'].idxmax()
        df_clean = df.loc[latest_indices]

        # 4. מציאת הבדיקה שמתאימה לזמן המבוקש (ValidTime)
        if query_valid_time.hour == 0 and query_valid_time.minute == 0:
            # אם לא צוינה שעה, מחפשים את האחרונה באותו יום
            df_final = df_clean[df_clean['ValidStartTime'].dt.date == query_valid_time.date()]
        else:
            # אם צוינה שעה, מחפשים בדיקה שקרתה בדיוק אז או לפני
            df_final = df_clean[df_clean['ValidStartTime'] <= query_valid_time]

        if df_final.empty:
            return "No test found matching valid time criteria."

        # שליפת השורה הרלוונטית (האחרונה שנמצאה)
        row = df_final.sort_values('ValidStartTime', ascending=False).iloc[0]

        # בדיקה אם השורה מסומנת כמחוקה
        if str(row['Value']) == "DELETED":
            return "Record was deleted."

        desc = self.get_loinc_desc(loinc)
        return f"*** RESULT: {row['Value']} *** (Date: {row['ValidStartTime']}, Concept: {desc})"

    # --- פונקציית מחיקה (לוגית) ---
    def operation_delete(self, first_name, last_name, loinc, valid_time_to_delete, delete_transaction_time=None):
        if delete_transaction_time is None:
            delete_transaction_time = datetime.now()

        print(f"\n--- DELETE: {first_name} {last_name}, Time: {valid_time_to_delete} ---")

        # אנחנו יוצרים שורה חדשה שהיא העתק של השורה המקורית,
        # אבל עם Value="DELETED" וזמן טרנזקציה עכשווי.

        # 1. מציאת השורה המקורית כדי לשכפל את פרטיה
        mask_patient = (self.db['FirstName'].astype(str).str.lower() == first_name.lower()) & \
                       (self.db['LastName'].astype(str).str.lower() == last_name.lower())
        mask_loinc = self.db['LOINC'].astype(str) == str(loinc)

        df_candidates = self.db[mask_patient & mask_loinc]

        # חיפוש לפי תאריך
        if valid_time_to_delete.hour == 0 and valid_time_to_delete.minute == 0:
            df_match = df_candidates[df_candidates['ValidStartTime'].dt.date == valid_time_to_delete.date()]
        else:
            df_match = df_candidates[df_candidates['ValidStartTime'] == valid_time_to_delete]

        if df_match.empty:
            print("Error: Could not find record to delete.")
            return

        # לוקחים אחת מהשורות כדי להעתיק את המטא-דאטה (שם, קוד וכו')
        original_row = df_match.iloc[0].copy()

        # יצירת שורת המחיקה
        new_row = original_row.copy()
        new_row['TransactionTime'] = delete_transaction_time  # זמן המחיקה (עכשיו)
        new_row['ValidStartTime'] = valid_time_to_delete  # הזמן המקורי של המדידה שאנו מוחקים
        new_row['Value'] = "DELETED"  # הסימון

        # הוספה לדאטה-בייס
        self.db = pd.concat([self.db, pd.DataFrame([new_row])], ignore_index=True)
        print("Success: Record marked as DELETED.")

    # --- פונקציית עדכון ---
    def operation_update(self, first_name, last_name, loinc, old_valid_time, new_value, update_transaction_time=None):
        if update_transaction_time is None:
            update_transaction_time = datetime.now()

        print(f"\n--- UPDATE: {first_name} {last_name} -> New Value: {new_value} ---")

        # לוגיקה זהה למחיקה, רק שהערך הוא המספר החדש במקום "DELETED"
        mask_patient = (self.db['FirstName'].astype(str).str.lower() == first_name.lower()) & \
                       (self.db['LastName'].astype(str).str.lower() == last_name.lower())
        mask_loinc = self.db['LOINC'].astype(str) == str(loinc)

        df_candidates = self.db[mask_patient & mask_loinc]

        if old_valid_time.hour == 0 and old_valid_time.minute == 0:
            df_match = df_candidates[df_candidates['ValidStartTime'].dt.date == old_valid_time.date()]
        else:
            df_match = df_candidates[df_candidates['ValidStartTime'] == old_valid_time]

        if df_match.empty:
            print("Error: Could not find original record to update.")
            return

        original_row = df_match.iloc[0].copy()

        new_row = original_row.copy()
        new_row['TransactionTime'] = update_transaction_time
        new_row['ValidStartTime'] = old_valid_time  # שומרים על זמן המדידה המקורי
        new_row['Value'] = new_value

        self.db = pd.concat([self.db, pd.DataFrame([new_row])], ignore_index=True)
        print(f"Success: Updated value to {new_value}.")


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # אתחול
    sys = TemporalDB()

    # טעינת קובץ
    file_name = "project_db_2025.xlsx"
    sys.load_data(file_name)

