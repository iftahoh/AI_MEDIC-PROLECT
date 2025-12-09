import pandas as pd
from datetime import datetime
import os


class TemporalDB:
    def __init__(self):
        self.db = pd.DataFrame()
        self.loinc_dictionary = {
            "12345": "Leukocytes [#/volume] in Blood by Automated count",
            "14743-9": "Glucose [Moles/volume] in Body fluid",
            "11218-5": "Anatomic pathology & Lab medicine"
        }

    def resolve_test_identifier(self, identifier):
        """
        מקבל מזהה בדיקה מהמשתמש:
        - אם ריק → מחזיר None (בלי סינון לפי בדיקה)
        - אם נראה כמו קוד LOINC → מחזיר אותו כמו שהוא
        - אחרת מנסה לחפש לפי תיאור הבדיקה במילון loinc_dictionary
        """
        if identifier is None:
            return None

        identifier = identifier.strip()
        if identifier == "":
            return None

        # אם זה נראה כמו קוד LOINC (ספרות ו/או מקף) – נניח שזה הקוד עצמו
        raw = identifier.replace("-", "")
        if raw.isdigit():
            return identifier  # זה קוד LOINC

        # אחרת – נחפש לפי תיאור הבדיקה במילון (values)
        lower_id = identifier.lower()
        for code, desc in self.loinc_dictionary.items():
            # אם המילה שהמשתמש הקליד מופיעה בתוך התיאור
            if lower_id in desc.lower():
                return code

        # לא מצאנו שום דבר מתאים
        return None

    def load_data(self, file_path):
        """ טעינת נתונים חכמה """
        print(f"Attempting to load: {file_path}")

        if not os.path.exists(file_path):
            print(f"ERROR: File not found at {file_path}")
            return

        try:
            if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
                df = pd.read_excel(file_path)
            else:
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                except:
                    df = pd.read_csv(file_path, encoding='cp1255')

            df.columns = df.columns.str.strip().str.replace('"', '')

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

            time_cols = ['TransactionTime', 'ValidStartTime', 'ValidStopTime']
            for col in time_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

            self.db = df
            print(f"SUCCESS: Loaded {len(self.db)} records.")

        except Exception as e:
            print(f"CRITICAL ERROR during loading: {e}")

    def get_loinc_desc(self, loinc_code):
        return self.loinc_dictionary.get(str(loinc_code), "Unknown Concept")

    def query_retrieve(self, first_name, last_name, loinc, query_valid_time, query_transaction_time=None):
        if query_transaction_time is None:
            query_transaction_time = datetime.now()

        # סינון ראשוני
        if self.db.empty: return "Database is empty."

        mask_patient = (self.db['FirstName'].astype(str).str.lower() == first_name.lower()) & \
                       (self.db['LastName'].astype(str).str.lower() == last_name.lower())
        mask_loinc = self.db['LOINC'].astype(str) == str(loinc)

        df = self.db[mask_patient & mask_loinc].copy()

        if df.empty: return "No records found."

        # סינון לפי זמן טרנזקציה
        df = df[df['TransactionTime'] <= query_transaction_time]
        if df.empty: return "No data at that Transaction Time."

        # מציאת הגרסה העדכנית ביותר לכל מועד מדידה
        latest_indices = df.groupby('ValidStartTime')['TransactionTime'].idxmax()
        df_clean = df.loc[latest_indices]

        # מציאת הרשומה המתאימה לזמן המבוקש
        if query_valid_time.hour == 0 and query_valid_time.minute == 0:
            df_final = df_clean[df_clean['ValidStartTime'].dt.date == query_valid_time.date()]
        else:
            df_final = df_clean[df_clean['ValidStartTime'] <= query_valid_time]

        if df_final.empty: return "No matching record found."

        row = df_final.sort_values('ValidStartTime', ascending=False).iloc[0]

        if str(row['Value']) == "DELETED":
            return "Record was deleted."

        desc = self.get_loinc_desc(loinc)
        return f"*** RESULT: {row['Value']} *** (Date: {row['ValidStartTime']}, Concept: {desc})"

    def operation_delete(self, first_name, last_name, loinc, valid_time_to_delete, delete_transaction_time=None):
        if delete_transaction_time is None: delete_transaction_time = datetime.now()

        mask_patient = (self.db['FirstName'].astype(str).str.lower() == first_name.lower()) & \
                       (self.db['LastName'].astype(str).str.lower() == last_name.lower())
        mask_loinc = self.db['LOINC'].astype(str) == str(loinc)
        df_candidates = self.db[mask_patient & mask_loinc]

        if valid_time_to_delete.hour == 0 and valid_time_to_delete.minute == 0:
            df_match = df_candidates[df_candidates['ValidStartTime'].dt.date == valid_time_to_delete.date()]
        else:
            df_match = df_candidates[df_candidates['ValidStartTime'] == valid_time_to_delete]

        if df_match.empty:
            print("Error: Could not find record to delete.")
            return

        new_row = df_match.iloc[0].copy()
        new_row['TransactionTime'] = delete_transaction_time
        new_row['ValidStartTime'] = valid_time_to_delete
        new_row['Value'] = "DELETED"

        self.db = pd.concat([self.db, pd.DataFrame([new_row])], ignore_index=True)
        print("Success: Record marked as DELETED.")

    def operation_update(self, first_name, last_name, loinc, old_valid_time, new_value, update_transaction_time=None):
        if update_transaction_time is None: update_transaction_time = datetime.now()

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

        new_row = df_match.iloc[0].copy()
        new_row['TransactionTime'] = update_transaction_time
        new_row['ValidStartTime'] = old_valid_time
        new_row['Value'] = new_value

        self.db = pd.concat([self.db, pd.DataFrame([new_row])], ignore_index=True)
        print(f"Success: Updated value to {new_value}.")

    def query_history(self, first_name, last_name, start_valid_time, end_valid_time, query_transaction_time=None,
                      loinc=None):
        """
        שאילתת אחזור היסטוריה: מחזירה את כל המדידות התקפות בטווח הזמנים המבוקש.
        אם צוין LOINC - מסננת לפיו. אם לא - מחזירה את כל הבדיקות של החולה.
        """
        if query_transaction_time is None:
            query_transaction_time = datetime.now()

        # 1. סינון לפי חולה (חובה)
        if self.db.empty: return "Database is empty."

        mask_patient = (self.db['FirstName'].astype(str).str.lower() == first_name.lower()) & \
                       (self.db['LastName'].astype(str).str.lower() == last_name.lower())

        df = self.db[mask_patient].copy()

        # 2. סינון אופציונלי לפי LOINC (רק אם המשתמש הזין משהו)
        if loinc and str(loinc).strip():
            mask_loinc = df['LOINC'].astype(str) == str(loinc)
            df = df[mask_loinc]

        if df.empty: return "No records found for this patient (and LOINC criteria)."

        # 3. סינון לפי זמן טרנזקציה (Perspective Time)
        df = df[df['TransactionTime'] <= query_transaction_time]

        if df.empty: return "No data found visible at this Transaction Time."

        # 4. מציאת הגרסה העדכנית ביותר לכל בדיקה ולכל מועד
        # השינוי החשוב: מקבצים גם לפי LOINC כדי לא לערבב בין סוגי בדיקות שונים באותו זמן
        latest_indices = df.groupby(['LOINC', 'ValidStartTime'])['TransactionTime'].idxmax()
        df_clean = df.loc[latest_indices]

        # 5. סינון לפי טווח זמנים מבוקש (Valid Time Range)
        mask_time = (df_clean['ValidStartTime'] >= start_valid_time) & (df_clean['ValidStartTime'] <= end_valid_time)
        df_history = df_clean[mask_time].sort_values(['ValidStartTime', 'LOINC'])

        if df_history.empty: return "No matching records in the specified time range."

        # 6. עיצוב הפלט
        result_lines = [f"*** HISTORY RESULT for {first_name} {last_name} ***"]

        found_records = False
        for _, row in df_history.iterrows():
            val = row['Value']
            if str(val) == "DELETED":
                continue

            found_records = True
            v_time_str = row['ValidStartTime'].strftime('%Y-%m-%d %H:%M')
            curr_loinc = row['LOINC']
            desc = self.get_loinc_desc(curr_loinc)

            # הדפסה שתכלול גם את שם הבדיקה כי עכשיו זה יכול להיות מעורבב
            result_lines.append(
                f"Date: {v_time_str} | Test: {curr_loinc} ({desc}) | Value: {val} {row.get('Unit', '')}")

        if not found_records:
            return "No active records found in this range."

        return "\n".join(result_lines)