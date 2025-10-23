import urllib3 # הוסף את זה
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # הוסף את זה

import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime

# --- הגדרות קבועות ---
GTFS_URL = "https://gtfs.mot.gov.il/gtfsfiles/israel-public-transportation.zip"
TARGET_ROUTE_NAME = "20"
TARGET_CITY_STOP = "חריש" # נשתמש בזה כדי לסנן תחנות רלוונטיות
OUTPUT_FILENAME = "harish_bus20_schedule.json"

# --- פונקציות עזר ---

def download_and_extract_gtfs(url):
    """מוריד את קובץ ה-ZIP של ה-GTFS ומחלץ אותו לזיכרון (IO)"""
    print(f"1. מוריד קובץ GTFS מ: {url}")
    response = requests.get(url, stream=True, verify=False)
    response.raise_for_status() # בדיקה אם ההורדה הצליחה
    
    # משתמשים ב-io.BytesIO כדי לעבוד עם הקובץ בזיכרון, ללא שמירה פיזית
    zip_in_memory = io.BytesIO(response.content)
    print("   ההורדה הושלמה בהצלחה.")
    return zipfile.ZipFile(zip_in_memory)

def process_gtfs_data(zf):
    """קורא את הקבצים הרלוונטיים ומבצע סינון לוחות זמנים"""
    print("2. מתחיל בעיבוד נתונים...")
    
    # 2.1. קריאת קובצי ליבה
    routes = pd.read_csv(zf.open('routes.txt'))
    stops = pd.read_csv(zf.open('stops.txt'))
    trips = pd.read_csv(zf.open('trips.txt'))
    stop_times = pd.read_csv(zf.open('stop_times.txt'))
    calendar = pd.read_csv(zf.open('calendar.txt'))
    
    # 2.2. סינון קו 20 (Route 20)
    target_route = routes[routes['route_short_name'] == TARGET_ROUTE_NAME]
    if target_route.empty:
        raise ValueError(f"לא נמצא קו בשם: {TARGET_ROUTE_NAME}")
    target_route_id = target_route['route_id'].iloc[0]
    
    # 2.3. סינון נסיעות (Trips) של קו 20
    target_trips = trips[trips['route_id'] == target_route_id]
    
    # 2.4. סינון תחנות (Stops) לפי "חריש"
    # המשרד לא תמיד מזהה לפי שם העיר, אך ננסה לסנן לפי תחנות שיש בשמן 'חריש'
    harish_stops = stops[stops['stop_name'].str.contains(TARGET_CITY_STOP, na=False)]
    harish_stop_ids = harish_stops['stop_id'].unique()
    
    if len(harish_stop_ids) == 0:
        print(f"אזהרה: לא נמצאו תחנות עם השם '{TARGET_CITY_STOP}'. עובר לסינון גס.")
        # במקרה שאין התאמה מדויקת, נחפש לפי תחנות שבהן קו 20 עובר, ונצפה לעיבוד ידני.
        all_trip_ids_for_route = target_trips['trip_id'].unique()
        target_stop_times = stop_times[stop_times['trip_id'].isin(all_trip_ids_for_route)]
    else:
        # 2.5. סינון זמני עצירה (Stop Times) של קו 20, רק בתחנות חריש
        target_stop_times = stop_times[
            (stop_times['trip_id'].isin(target_trips['trip_id'].unique())) &
            (stop_times['stop_id'].isin(harish_stop_ids))
        ]
    
    # 2.6. שילוב הנתונים לקבלת תוצאה סופית
    merged_data = pd.merge(target_stop_times, target_trips, on='trip_id')
    merged_data = pd.merge(merged_data, harish_stops[['stop_id', 'stop_name']], on='stop_id', how='left')

    # 2.7. סינון לפי יום בשבוע הנוכחי (today's day of the week)
    today_weekday = datetime.now().strftime('%A').lower() # לדוגמה: monday
    
    # יצירת מפת קישור בין יום בשבוע לשדה ב-calendar.txt
    day_mapping = {
        'monday': 'monday', 'tuesday': 'tuesday', 'wednesday': 'wednesday',
        'thursday': 'thursday', 'friday': 'friday', 'saturday': 'saturday', 'sunday': 'sunday'
    }
    
    calendar_today = calendar[calendar[day_mapping[today_weekday]] == 1]
    
    # סינון הנתונים לפי הנסיעות הפעילות היום
    final_schedule = merged_data[merged_data['service_id'].isin(calendar_today['service_id'])]
    
    # 2.8. בחירת והצגת עמודות נקיות
    final_schedule = final_schedule[[
        'departure_time', 'stop_name', 'trip_id', 'stop_sequence', 'direction_id'
    ]].sort_values(by=['departure_time', 'stop_sequence'])
    
    # המרת הפלט לפורמט JSON נקי ונוח
    results = final_schedule.to_dict('records')
    print(f"3. עיבוד הושלם. נמצאו {len(results)} זמני יציאה רלוונטיים היום.")
    return results

# --- פונקציית ריצה ראשית ---
if __name__ == "__main__":
    try:
        # 1. הורדה ופתיחה
        zip_file_obj = download_and_extract_gtfs(GTFS_URL)
        
        # 2. עיבוד וסינון
        schedule_data = process_gtfs_data(zip_file_obj)
        
        # 3. שמירת הפלט לקובץ JSON
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            json.dump(schedule_data, f, ensure_ascii=False, indent=4)
        
        print(f"4. נתונים נשמרו בהצלחה לקובץ: {OUTPUT_FILENAME}")
        
    except Exception as e:
        print(f"שגיאה קריטית: {e}")
        # אם משהו נכשל, נשמור קובץ JSON ריק כדי לא לשבור את האתר
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            json.dump({"error": str(e), "note": "Failed to update schedule data."}, f, ensure_ascii=False, indent=4)
