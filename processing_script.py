import urllib3 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 

import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime

# --- הגדרות קבועות (מעודכנות לחיפוש מדויק) ---
GTFS_URL = "https://gtfs.mot.gov.il/gtfsfiles/israel-public-transportation.zip"
TARGET_ROUTES = ["20", "20א"] # קווים רלוונטיים
OUTPUT_FILENAME = "harish_bus20_schedule.json"

# רשימה מדויקת של כל התחנות הרלוונטיות בחריש
TARGET_STOP_NAMES = [
    "מסוף חריש/איסוף",  # תחנת יציאה אופיינית של 20
    "קידמה/התמדה",      # תחנת יציאה אופיינית של 20א
]

# --- פונקציות עזר (ללא שינוי מהותי) ---

def download_and_extract_gtfs(url):
    """מוריד את קובץ ה-ZIP של ה-GTFS ומחלץ אותו לזיכרון (IO)"""
    print(f"1. מוריד קובץ GTFS מ: {url}")
    response = requests.get(url, stream=True, verify=False)
    response.raise_for_status() 
    
    zip_in_memory = io.BytesIO(response.content)
    print("   ההורדה הושלמה בהצלחה.")
    return zipfile.ZipFile(zip_in_memory)

def process_gtfs_data(zf):
    """קורא את הקבצים הרלוונטיים ומבצע סינון לוחות זמנים"""
    print("2. מתחיל בעיבוד נתונים...")
    
    # 2.1. קריאת קובצי ליבה
    routes = pd.read_csv(zf.open('routes.txt'), dtype={'route_short_name': str})
    stops = pd.read_csv(zf.open('stops.txt'))
    trips = pd.read_csv(zf.open('trips.txt'))
    stop_times = pd.read_csv(zf.open('stop_times.txt'))
    calendar = pd.read_csv(zf.open('calendar.txt'))
    
    # 2.2. סינון קווים 20 ו-20א
    print(f"   מחפש קווים {TARGET_ROUTES}...")
    target_route = routes[
        routes['route_short_name'].isin(TARGET_ROUTES)
    ]

    if target_route.empty:
        print(f"3. עיבוד הושלם. נמצאו 0 זמני יציאה. סיבה: לא נמצאו קווים: {TARGET_ROUTES}")
        return []
        
    target_route_ids = target_route['route_id'].unique()
    target_trips = trips[trips['route_id'].isin(target_route_ids)]
    
    # 2.3. סינון תחנות לפי רשימת השמות המדויקת
    print(f"   מחפש תחנות לפי שמות מדויקים...")
    harish_stops = stops[
        stops['stop_name'].isin(TARGET_STOP_NAMES) # *** שינוי מרכזי כאן! ***
    ]
    harish_stop_ids = harish_stops['stop_id'].unique()
    
    if len(harish_stop_ids) == 0:
        print(f"3. עיבוד הושלם. נמצאו 0 זמני יציאה. סיבה: לא נמצאו תחנות תואמות ברשימה המדויקת.")
        return []

    # 2.4. סינון זמני עצירה של קו 20/20א העוברות בתחנות הנבחרות
    target_trip_ids = target_trips['trip_id'].unique()
    target_stop_times = stop_times[
        (stop_times['trip_id'].isin(target_trip_ids)) &
        (stop_times['stop_id'].isin(harish_stop_ids))
    ]

    if target_stop_times.empty:
        print(f"3. עיבוד הושלם. נמצאו 0 זמני יציאה. סיבה: אף קו מ-{TARGET_ROUTES} אינו עוצר בתחנות אלה היום.")
        return []

    # 2.5. שילוב הנתונים
    merged_data = pd.merge(target_stop_times, target_trips, on='trip_id')
    merged_data = pd.merge(merged_data, stops[['stop_id', 'stop_name']], on='stop_id', how='left')

    # 2.6. סינון לפי יום בשבוע הנוכחי
    today_weekday = datetime.now().strftime('%A').lower() 
    day_mapping = {
        'monday': 'monday', 'tuesday': 'tuesday', 'wednesday': 'wednesday',
        'thursday': 'thursday', 'friday': 'friday', 'saturday': 'saturday', 'sunday': 'sunday'
    }
    calendar_today = calendar[calendar[day_mapping[today_weekday]] == 1]
    final_schedule = merged_data[merged_data['service_id'].isin(calendar_today['service_id'])]
    
    # 2.7. בחירת והצגת עמודות נקיות ומיון
    final_schedule = final_schedule[[
        'route_short_name', 'departure_time', 'stop_name', 'direction_id'
    ]].sort_values(by=['departure_time', 'stop_name'])
    
    results = final_schedule.to_dict('records')
    print(f"3. עיבוד הושלם. נמצאו {len(results)} זמני יציאה רלוונטיים היום לקווים {TARGET_ROUTES}.")
    return results

# --- פונקציית ריצה ראשית (נשארת ללא שינוי בבלוק ה-if) ---
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
        print(f"שגיאה קריטית במהלך עיבוד: {e}")
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            json.dump({"error": str(e), "note": "Processing failed. Check log for details."}, f, ensure_ascii=False, indent=4)
        exit(1)