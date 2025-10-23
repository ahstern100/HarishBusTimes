import urllib3 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 

import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime

# --- הגדרות קבועות ---
GTFS_URL = "https://gtfs.mot.gov.il/gtfsfiles/israel-public-transportation.zip"
TARGET_ROUTE_NAME = "20"
TARGET_CITY_STOP = "חריש" 
OUTPUT_FILENAME = "harish_bus20_schedule.json"

# --- פונקציות עזר ---

def download_and_extract_gtfs(url):
    """מוריד את קובץ ה-ZIP של ה-GTFS ומחלץ אותו לזיכרון (IO)"""
    print(f"1. מוריד קובץ GTFS מ: {url}")
    # הפתרון לשגיאת SSL
    response = requests.get(url, stream=True, verify=False)
    response.raise_for_status() 
    
    zip_in_memory = io.BytesIO(response.content)
    print("   ההורדה הושלמה בהצלחה.")
    return zipfile.ZipFile(zip_in_memory)

def process_gtfs_data(zf):
    """קורא את הקבצים הרלוונטיים ומבצע סינון לוחות זמנים"""
    print("2. מתחיל בעיבוד נתונים...")
    
    # 2.1. קריאת קובצי ליבה
    routes = pd.read_csv(zf.open('routes.txt'), dtype={'route_short_name': str}) # קריאת מספר קו כמחרוזת
    stops = pd.read_csv(zf.open('stops.txt'))
    trips = pd.read_csv(zf.open('trips.txt'))
    stop_times = pd.read_csv(zf.open('stop_times.txt'))
    calendar = pd.read_csv(zf.open('calendar.txt'))
    
    # 2.2. תיקון: סינון גמיש לקו 20
    print(f"   מחפש קו {TARGET_ROUTE_NAME}...")
    target_route = routes[
        routes['route_short_name'].str.contains(f'^{TARGET_ROUTE_NAME}$', na=False, regex=True)
    ]
    
    # אם לא נמצא בדיוק, נחפש עם רווחים
    if target_route.empty:
         target_route = routes[
            routes['route_short_name'].str.contains(f'{TARGET_ROUTE_NAME}', na=False)
        ]

    if target_route.empty:
        raise ValueError(f"לא נמצא קו בשם: {TARGET_ROUTE_NAME}. ייתכן שמספר הקו השתנה.")
        
    target_route_id = target_route['route_id'].iloc[0]
    
    # 2.3. סינון נסיעות (Trips) של קו 20
    target_trips = trips[trips['route_id'] == target_route_id]
    
    # 2.4. סינון תחנות (Stops) לפי "חריש"
    print(f"   מחפש תחנות ב-'{TARGET_CITY_STOP}'...")
    harish_stops = stops[
        stops['stop_name'].str.contains(TARGET_CITY_STOP, na=False)
    ]
    harish_stop_ids = harish_stops['stop_id'].unique()
    
    if len(harish_stop_ids) == 0:
        raise ValueError(f"לא נמצאו תחנות עם השם '{TARGET_CITY_STOP}'. לא ניתן לסנן נסיעות.")

    # 2.5. סינון זמני עצירה (Stop Times) של קו 20, רק בתחנות חריש
    target_trip_ids = target_trips['trip_id'].unique()
    target_stop_times = stop_times[
        (stop_times['trip_id'].isin(target_trip_ids)) &
        (stop_times['stop_id'].isin(harish_stop_ids))
    ]
    
    # 2.6. שילוב הנתונים לקבלת תוצאה סופית
    # כדי לקבל את שם התחנה
    merged_data = pd.merge(target_stop_times, target_trips, on='trip_id')
    merged_data = pd.merge(merged_data, stops[['stop_id', 'stop_name']], on='stop_id', how='left')

    # 2.7. סינון לפי יום בשבוע הנוכחי 
    today_weekday = datetime.now().strftime('%A').lower() 
    
    day_mapping = {
        'monday': 'monday', 'tuesday': 'tuesday', 'wednesday': 'wednesday',
        'thursday': 'thursday', 'friday': 'friday', 'saturday': 'saturday', 'sunday': 'sunday'
    }
    
    calendar_today = calendar[calendar[day_mapping[today_weekday]] == 1]
    
    final_schedule = merged_data[merged_data['service_id'].isin(calendar_today['service_id'])]
    
    # 2.8. בחירת והצגת עמודות נקיות ומיון
    final_schedule = final_schedule[[
        'departure_time', 'stop_name', 'trip_id', 'stop_sequence', 'direction_id'
    ]].sort_values(by=['departure_time', 'stop_sequence'])
    
    # המרת הפלט לפורמט JSON נקי ונוח
    results = final_schedule.to_dict('records')
    print(f"3. עיבוד הושלם. נמצאו {len(results)} זמני יציאה רלוונטיים היום לקו 20 חריש.")
    return results

# --- פונקציית ריצה ראשית (מעודכנת) ---
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
        # שינוי: מעלים את השגיאה כדי שתופיע ביומן ה-Action ותחזיר exit code שאינו 0
        print(f"שגיאה קריטית במהלך עיבוד: {e}")
        # יצירת קובץ שגיאה נקי
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            json.dump({"error": str(e), "note": "Processing failed. Check log for details."}, f, ensure_ascii=False, indent=4)
        
        # *** חשוב: גורם ל-Action להיכשל באופן רשמי ***
        exit(1)
