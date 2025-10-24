import os
import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime
from typing import List, Dict, Any

# --- 1. מודול Setup וקריאת נתונים ---

GTFS_URL = "https://gtfs.mot.gov.il/gtfsfiles/israel-public-transportation.zip"
OUTPUT_FILENAME = "harish_multi_route_schedule.json"

# קריאת נתונים ממשתני סביבה או שימוש בערכי ברירת מחדל
# Note: משתני סביבה יוגדרו בקובץ main.yml
TARGET_ROUTES_STR = os.environ.get('TARGET_ROUTES', "20,20א,22,60,60א,71,71א,632,634,942,160,163,63")
TARGET_STOPS_STR = os.environ.get('TARGET_STOPS', "43898,43899,43897,43334,43496,40662")

TARGET_ROUTES: List[str] = [r.strip() for r in TARGET_ROUTES_STR.split(',')]
TARGET_STOPS: List[int] = [int(s.strip()) for s in TARGET_STOPS_STR.split(',')]

def download_and_extract_gtfs(url: str) -> zipfile.ZipFile:
    """מוריד את קובץ ה-ZIP ומחלץ אותו לזיכרון (עם עקיפת SSL)"""
    print(f"[SETUP] 1. מוריד קובץ GTFS מ: {url}")
    # שימוש ב-verify=False כדי לעקוף שגיאות SSL ב-GitHub Actions
    response = requests.get(url, stream=True, verify=False)
    response.raise_for_status() 
    
    zip_in_memory = io.BytesIO(response.content)
    return zipfile.ZipFile(zip_in_memory)

def load_gtfs_files(zf: zipfile.ZipFile) -> Dict[str, pd.DataFrame]:
    """טוען את קבצי ה-GTFS הרלוונטיים ל-Pandas DataFrames"""
    print("[SETUP] 2. טוען קבצי GTFS...")
    return {
        'routes': pd.read_csv(zf.open('routes.txt'), dtype={'route_short_name': str}),
        'stops': pd.read_csv(zf.open('stops.txt')),
        'trips': pd.read_csv(zf.open('trips.txt')),
        'stop_times': pd.read_csv(zf.open('stop_times.txt')),
        'calendar': pd.read_csv(zf.open('calendar.txt'))
    }

# --- 2. מודול Core Logic ---

def get_today_service_ids(calendar: pd.DataFrame) -> List[str]:
    """מוצא את מזהי השירות (Service IDs) שפעילים היום"""
    
    # 1. מציאת שם היום באנגלית (לדוגמה: friday)
    today_weekday = datetime.now().strftime('%A').lower() 
    
    # 2. מיפוי שם היום לשם השדה ב-GTFS (במקרה זה, זה זהה)
    day_mapping = {
        'monday': 'monday', 'tuesday': 'tuesday', 'wednesday': 'wednesday',
        'thursday': 'thursday', 'friday': 'friday', 'saturday': 'saturday', 'sunday': 'sunday'
    }
    
    day_column = day_mapping.get(today_weekday, None)
    
    if day_column is None or day_column not in calendar.columns:
        print(f"[CORE] שגיאה: לא ניתן למפות את היום '{today_weekday}' לעמודת ה-GTFS.")
        return []
    
    print(f"[CORE] מסנן Service IDs עבור היום: {day_column}")
    
    # סינון לפי יום בשבוע (שוויון ל-1)
    calendar_today = calendar[calendar[day_column] == 1]
    
    return calendar_today['service_id'].unique().tolist()



def find_departure_schedules(gtfs_data: Dict[str, pd.DataFrame], service_ids: List[str]) -> List[Dict[str, Any]]:
    """מוצא את זמני היציאה מתחנות היעד עבור הקווים הפעילים היום"""
    
    routes = gtfs_data['routes']
    trips = gtfs_data['trips']
    stop_times = gtfs_data['stop_times']
    stops = gtfs_data['stops']
    
    # 2.1. סינון נסיעות פעילות ורלוונטיות
    print(f"[CORE] 3. מסנן קווים רלוונטיים ({len(TARGET_ROUTES)} קווים)...")
    target_routes_df = routes[routes['route_short_name'].isin(TARGET_ROUTES)]
    target_route_ids = target_routes_df['route_id'].unique().tolist()
    
    # סינון נסיעות (Trips) של הקווים הנבחרים, שפעילות היום
    target_trips = trips[
        (trips['route_id'].isin(target_route_ids)) &
        (trips['service_id'].isin(service_ids))
    ].copy()
    
    if target_trips.empty:
        print("[CORE]   **אזהרה:** לא נמצאו נסיעות פעילות היום עבור הקווים המבוקשים.")
        return []

    # 2.2. מציאת זמני יציאה בתחנות היעד
    print(f"[CORE] 4. מוצא זמני יציאה מתחנות היעד...")
    
    # *** התיקון העיקרי: מיזוג ישיר בין הנסיעות הפעילות לזמני העצירה בתחנות היעד ***
    
    # כל זמני העצירה בתחנות היעד שלך
    relevant_stop_times = stop_times[
        stop_times['stop_id'].isin(TARGET_STOPS) 
    ].copy()
    
    # חיתוך (AND) בין הנסיעות הפעילות לזמני העצירה בתחנות היעד
    final_relevant_stop_times = relevant_stop_times[
        relevant_stop_times['trip_id'].isin(target_trips['trip_id'])
    ].copy()
    
    if final_relevant_stop_times.empty:
        # הודעה זו היא המדויקת ביותר למצב הנוכחי:
        print("[CORE]   **אזהרה:** אף נסיעה פעילה היום (יום שישי) אינה עוצרת בתחנות היעד שצוינו.")
        return []
    
    # 2.3. איחוד הנתונים וסינון כפילויות
    
    # מיזוג עם פרטי הנסיעה והקו
    merged_data = pd.merge(final_relevant_stop_times, target_trips, on='trip_id')
    merged_data = pd.merge(merged_data, routes[['route_id', 'route_short_name']], on='route_id')
    merged_data = pd.merge(merged_data, stops[['stop_id', 'stop_name']], on='stop_id', how='left')

    # סינון כפילויות: אם אותו קו יוצא באותה שעה מאותה תחנה
    unique_departures = merged_data.drop_duplicates(
        subset=['route_short_name', 'departure_time', 'stop_id'], 
        keep='first'
    )
    
    # 2.4. יצירת הפלט הסופי
    print(f"[CORE] 5. נמצאו {len(unique_departures)} יציאות ייחודיות.")
    
    # סידור הנתונים
    unique_departures = unique_departures[[
        'route_short_name', 
        'departure_time', 
        'stop_name', 
        'direction_id',
        'stop_sequence'
    ]].sort_values(by=['route_short_name', 'departure_time'])
    
    return unique_departures.to_dict('records')


# --- 3. מודול Output ופונקציה ראשית ---

def main():
    """פונקציה ראשית שמנהלת את התהליך כולו"""
    # הדפסת הפרמטרים שנבחרו
    print("-" * 50)
    print(f"[MAIN] קווים נבחרים: {TARGET_ROUTES}")
    print(f"[MAIN] תחנות יעד (IDs): {TARGET_STOPS}")
    print("-" * 50)

    try:
        # 1. טעינת נתונים
        zip_file_obj = download_and_extract_gtfs(GTFS_URL)
        gtfs_data = load_gtfs_files(zip_file_obj)
        
        # *** DEBUG: הדפסת שמות התחנות שנטענו ***
        stops_df = gtfs_data['stops']
        found_stops = stops_df[stops_df['stop_id'].isin(TARGET_STOPS)]
        
        if found_stops.empty:
             raise ValueError("אף אחד מ-Stop ID שהוזנו לא נמצא בקובץ stops.txt. ודא שה-IDs נכונים.")
             
        print(f"[DEBUG] שמות התחנות שנטענו: {found_stops['stop_name'].unique().tolist()}")
        # *****************************************
        
        # 2. עיבוד לוגי - ביטול סינון יום לחלוטין (כדי למצוא את כל התוצאות האפשריות)
        service_ids = gtfs_data['calendar']['service_id'].unique().tolist()
        print(f"[CORE] נמצאו {len(service_ids)} ALL Service IDs (DEBUG MODE - ALL DAYS).")
        
        schedule_data = find_departure_schedules(gtfs_data, service_ids)

        # 3. שמירת הפלט
        print(f"[OUTPUT] שומר {len(schedule_data)} רשומות לקובץ {OUTPUT_FILENAME}...")
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            # הוספת כותרת ל-JSON
            final_output = {
                "update_time": datetime.now().isoformat(),
                "query_routes": TARGET_ROUTES,
                "query_stops": TARGET_STOPS,
                "results": schedule_data
            }
            json.dump(final_output, f, ensure_ascii=False, indent=4)
        
        print(f"[MAIN] 🌟 סיום מוצלח! נתונים נשמרו בהצלחה.")
        
    except Exception as e:
        print(f"[MAIN] ❌ שגיאה קריטית: {e}")
        # שמירת קובץ שגיאה גם במקרה של כשל
        error_output = {
            "update_time": datetime.now().isoformat(),
            "error": str(e),
            "note": "Processing failed. Check GitHub Actions log for full traceback."
        }
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            json.dump(error_output, f, ensure_ascii=False, indent=4)
        exit(1)

if __name__ == "__main__":
    # השתקת אזהרות SSL אם קיימות (רלוונטי לסביבת לינוקס)
    try:
        import urllib3 
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except:
        pass # התעלמות אם הספרייה לא קיימת
        
    main()