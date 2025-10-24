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
    """מוצא את לוחות הזמנים המבוקשים ומזהה תחנות מוצא"""
    
    routes = gtfs_data['routes']
    trips = gtfs_data['trips']
    stop_times = gtfs_data['stop_times']
    stops = gtfs_data['stops']
    
    final_results = []
    

    # 2.1. סינון נסיעות פעילות ורלוונטיות
    print(f"[CORE] 3. מסנן קווים רלוונטיים ({len(TARGET_ROUTES)} קווים)...")
    target_routes_df = routes[routes['route_short_name'].isin(TARGET_ROUTES)]
    target_route_ids = target_routes_df['route_id'].unique().tolist()
    
    # כל הנסיעות (Trip IDs) של הקווים האלה שפעילות היום
    target_trips = trips[
        (trips['route_id'].isin(target_route_ids)) &
        (trips['service_id'].isin(service_ids))
    ].copy()
    
    # *** DEBUG 1: כמה נסיעות פעילות היום? ***
    print(f"[DEBUG 1] נמצאו {len(target_trips)} נסיעות פעילות היום בקווים הנבחרים.")
    # *****************************************
    
    if target_trips.empty:
        print("[CORE]   **אזהרה:** לא נמצאו נסיעות פעילות היום עבור הקווים המבוקשים.")
        return []

    # 2.2. זיהוי נסיעות שעוברות בתחנות היעד
    # מיזוג עם stop_times כדי לדעת אילו נסיעות עוברות בתחנות שלנו
    relevant_stop_times = stop_times[
        stop_times['stop_id'].isin(TARGET_STOPS)
    ].copy()
    
    relevant_trip_ids = relevant_stop_times['trip_id'].unique()
    
    # *** DEBUG 2: כמה נסיעות עוברות בתחנות היעד שלך (כל יום)? ***
    print(f"[DEBUG 2] נמצאו {len(relevant_trip_ids)} נסיעות שעוברות בתחנות היעד (בכל יום).")
    # *****************************************
    
    # סינון ה-Trips שגם רלוונטיים (היום) וגם עוברים בתחנות שלנו
    final_relevant_trips = target_trips[
        target_trips['trip_id'].isin(relevant_trip_ids)
    ].copy()
    
    # *** DEBUG 3: גודל החיתוך הסופי ***
    print(f"[DEBUG 3] גודל החיתוך הסופי (נסיעות פעילות שעוברות בתחנות) הוא: {len(final_relevant_trips)}")
    # **********************************
    
    if final_relevant_trips.empty:
        print("[CORE]   **אזהרה:** החיתוך ריק. אין חפיפה בין הקווים הפעילים לתחנות היעד.")
        return []


    # 2.3. מציאת תחנת המוצא של כל נסיעה שנמצאה
    print(f"[CORE] 4. מוצא תחנות מוצא עבור {len(final_relevant_trips)} נסיעות רלוונטיות...")
    
    # מוצאים את תחנת המוצא (stop_sequence = 1) של כל נסיעה
    origin_stop_times = stop_times[
        (stop_times['trip_id'].isin(final_relevant_trips['trip_id'])) &
        (stop_times['stop_sequence'] == 1)
    ].copy()
    
    # 2.4. איחוד הנתונים וסינון כפילויות
    
    # מיזוג עם פרטי הנסיעה והקו
    merged_data = pd.merge(origin_stop_times, final_relevant_trips, on='trip_id')
    merged_data = pd.merge(merged_data, routes[['route_id', 'route_short_name']], on='route_id')
    merged_data = pd.merge(merged_data, stops[['stop_id', 'stop_name']], on='stop_id', how='left')

    # סינון כפילויות: אם אותו קו (route_short_name) יוצא באותה שעה (departure_time)
    # מאותה תחנה, זו כפילות שנוצרה ממיזוג הנתונים, אז נשמור רק את הייחודיים.
    unique_departures = merged_data.drop_duplicates(
        subset=['route_short_name', 'departure_time', 'stop_id'], 
        keep='first'
    )
    
    # 2.5. יצירת הפלט הסופי
    print(f"[CORE] 5. נמצאו {len(unique_departures)} יציאות ייחודיות.")
    
    # סידור הנתונים
    unique_departures = unique_departures[[
        'route_short_name', 
        'departure_time', 
        'stop_name', 
        'trip_id', 
        'direction_id'
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
        
        # 2. עיבוד לוגי
        service_ids = get_today_service_ids(gtfs_data['calendar'])
        print(f"[CORE] נמצאו {len(service_ids)} Service IDs פעילים היום.")
        
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