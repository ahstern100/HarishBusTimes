import os
import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime
from typing import List, Dict, Any

# --- הגדרות נתיבים ---
GTFS_URL = "https://gtfs.mot.gov.il/gtfsfiles/israel-public-transportation.zip"
OUTPUT_FILENAME = "harish_multi_route_schedule.json"
EXTRACT_FOLDER = "extractedGtfs"
INPUT_VIBE_FILE = "VibeCodeInput.txt"

def get_target_routes_from_file() -> List[str]:
    """קורא את מספרי הקווים מהקובץ במבנה: מספר קו | מספר תחנה ."""
    routes = []
    if not os.path.exists(INPUT_VIBE_FILE):
        print(f"[SETUP] אזהרה: הקובץ {INPUT_VIBE_FILE} לא נמצא.")
        return []
    
    print(f"[SETUP] קורא קווים מתוך {INPUT_VIBE_FILE}...")
    with open(INPUT_VIBE_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                # פיצול לפי הקו האנכי (|) ולקיחת החלק הראשון (מספר הקו)
                parts = line.split('|')
                if parts:
                    route_num = parts[0].strip()
                    if route_num:
                        routes.append(route_num)
    
    # הסרת כפילויות אם יש
    routes = list(set(routes))
    print(f"[SETUP] קווים שזוהו: {routes}")
    return routes

def download_and_extract_gtfs(url: str) -> zipfile.ZipFile:
    """מוריד את ה-ZIP לזיכרון (עם עקיפת SSL)"""
    print(f"[SETUP] 1. מוריד קובץ GTFS מהשרת...")
    response = requests.get(url, stream=True, verify=False)
    response.raise_for_status() 
    return zipfile.ZipFile(io.BytesIO(response.content))

def process_and_save_filtered_gtfs(zf: zipfile.ZipFile, target_routes: List[str]):
    """מחלץ, מסנן ושומר רק מה שרלוונטי לקווים שנבחרו"""
    print(f"[SETUP] 2. מתחיל סינון ושמירה לתיקייה {EXTRACT_FOLDER}...")
    
    if not os.path.exists(EXTRACT_FOLDER):
        os.makedirs(EXTRACT_FOLDER)

    # 1. טעינת routes וסינון לפי הקווים שחולצו מהקובץ
    routes = pd.read_csv(zf.open('routes.txt'), dtype={'route_short_name': str})
    filtered_routes = routes[routes['route_short_name'].isin(target_routes)]
    
    if filtered_routes.empty:
        print(f"[!] אזהרה: לא נמצאו קווים תואמים ב-GTFS עבור הרשימה: {target_routes}")
    
    filtered_routes.to_csv(os.path.join(EXTRACT_FOLDER, 'routes.txt'), index=False)
    target_route_ids = filtered_routes['route_id'].unique()

    # 2. טעינת trips וסינון
    trips = pd.read_csv(zf.open('trips.txt'))
    filtered_trips = trips[trips['route_id'].isin(target_route_ids)]
    filtered_trips.to_csv(os.path.join(EXTRACT_FOLDER, 'trips.txt'), index=False)
    target_trip_ids = filtered_trips['trip_id'].unique()

    # 3. טעינת stop_times וסינון (בצ'אנקים כדי לחסוך זיכרון)
    print(f"[SETUP] מעבד את stop_times.txt (זה עשוי לקחת דקה)...")
    stop_times_iterator = pd.read_csv(zf.open('stop_times.txt'), chunksize=200000)
    
    first_chunk = True
    for chunk in stop_times_iterator:
        filtered_chunk = chunk[chunk['trip_id'].isin(target_trip_ids)]
        mode = 'w' if first_chunk else 'a'
        header = True if first_chunk else False
        filtered_chunk.to_csv(os.path.join(EXTRACT_FOLDER, 'stop_times.txt'), mode=mode, header=header, index=False)
        first_chunk = False

    # 4. שמירת קבצים קטנים ללא שינוי
    for filename in ['stops.txt', 'calendar.txt']:
        with zf.open(filename) as source, open(os.path.join(EXTRACT_FOLDER, filename), 'wb') as target:
            target.write(source.read())
            
    print(f"[SETUP] סיום. קבצים נשמרו.")
    
    # טעינה מחדש לצורך העיבוד של ה-JSON
    return {
        'routes': filtered_routes,
        'trips': filtered_trips,
        'stop_times': pd.read_csv(os.path.join(EXTRACT_FOLDER, 'stop_times.txt')),
        'stops': pd.read_csv(os.path.join(EXTRACT_FOLDER, 'stops.txt')),
        'calendar': pd.read_csv(os.path.join(EXTRACT_FOLDER, 'calendar.txt'))
    }

def convert_codes_to_ids(stops_df: pd.DataFrame, target_codes: List[str]) -> List[int]:
    target_codes_str = [str(c) for c in target_codes]
    found_stops = stops_df[stops_df['stop_code'].astype(str).isin(target_codes_str)]
    return found_stops['stop_id'].unique().tolist()

def get_today_service_ids(calendar: pd.DataFrame) -> List[str]:
    # מציאת היום הנוכחי (למשל 'sunday', 'monday'...)
    today_weekday = datetime.now().strftime('%A').lower() 
    if today_weekday not in calendar.columns:
        return []
    calendar_today = calendar[calendar[today_weekday] == 1]
    return calendar_today['service_id'].unique().tolist()

def find_departure_schedules(gtfs_data: Dict[str, pd.DataFrame], service_ids: List[str], target_stop_ids: List[int]) -> List[Dict[str, Any]]:
    routes = gtfs_data['routes']
    trips = gtfs_data['trips']
    stop_times = gtfs_data['stop_times']
    stops = gtfs_data['stops']
    
    target_trips = trips[
        (trips['route_id'].isin(routes['route_id'])) &
        (trips['service_id'].isin(service_ids))
    ].copy()
    
    relevant_stop_times = stop_times[stop_times['stop_id'].isin(target_stop_ids)].copy()
    final_relevant_stop_times = relevant_stop_times[relevant_stop_times['trip_id'].isin(target_trips['trip_id'])]
    
    if final_relevant_stop_times.empty:
        return []
    
    merged_data = pd.merge(final_relevant_stop_times, target_trips, on='trip_id')
    merged_data = pd.merge(merged_data, routes[['route_id', 'route_short_name']], on='route_id')
    merged_data = pd.merge(merged_data, stops[['stop_id', 'stop_name']], on='stop_id', how='left')

    unique_departures = merged_data.drop_duplicates(subset=['route_short_name', 'departure_time', 'stop_id'])
    
    return unique_departures[[
        'route_short_name', 'departure_time', 'stop_name', 'direction_id'
    ]].sort_values(by=['departure_time']).to_dict('records')

def main():
    # 1. חילוץ קווים מהקובץ עם המבנה החדש
    target_routes = get_target_routes_from_file()
    
    if not target_routes:
        print("[MAIN] ❌ לא נמצאו קווים לעיבוד. וודא שקובץ VibeCodeInput.txt תקין.")
        return

    # משתני סביבה לתחנות (מה-YAML)
    target_stops_str = os.environ.get('TARGET_STOPS', "43898,43899,43897,43334,43496,40662")
    target_stop_codes = [s.strip() for s in target_stops_str.split(',')]

    try:
        # 2. הורדה, סינון ושמירה פיזית
        zip_obj = download_and_extract_gtfs(GTFS_URL)
        gtfs_data = process_and_save_filtered_gtfs(zip_obj, target_routes)
        
        # 3. יצירת ה-JSON עבור האפליקציה/אתר
        stop_ids = convert_codes_to_ids(gtfs_data['stops'], target_stop_codes)
        service_ids = get_today_service_ids(gtfs_data['calendar'])
        schedule_data = find_departure_schedules(gtfs_data, service_ids, stop_ids)

        final_output = {
            "update_time": datetime.now().isoformat(),
            "results": schedule_data
        }
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            json.dump(final_output, f, ensure_ascii=False, indent=4)
        
        print(f"[MAIN] 🌟 הצלחה! הקבצים המסוננים מוכנים בתיקייה {EXTRACT_FOLDER}.")
        
    except Exception as e:
        print(f"[MAIN] ❌ שגיאה כללית: {e}")
        exit(1)

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()
