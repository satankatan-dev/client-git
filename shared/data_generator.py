import numpy as np
import pandas as pd
from pathlib import Path
import json

def generate_precipitation_data(region_bounds, num_stations, output_path):
    """
    Генерирует CSV файл с данными об осадках для станций в указанном регионе
    """
    # Границы Массачусетса
    west = region_bounds['west']
    east = region_bounds['east']
    south = region_bounds['south']
    north = region_bounds['north']
    
    # Генерация случайных координат станций
    np.random.seed(42)  # Для воспроизводимости
    
    stations_lons = np.random.uniform(west, east, num_stations)
    stations_lats = np.random.uniform(south, north, num_stations)
    
    # Генерация данных об осадках (0-150 мм/сутки)
    # 365 дней для каждого года (временные срезы)
    precipitation_data = np.random.uniform(0, 150, (num_stations, 365))
    
    # Создание DataFrame
    data = []
    for station_id in range(num_stations):
        for day in range(365):
            data.append({
                'station_id': station_id,
                'longitude': stations_lons[station_id],
                'latitude': stations_lats[station_id],
                'day_of_year': day,
                'precipitation_mm': precipitation_data[station_id, day]
            })
    
    df = pd.DataFrame(data)
    
    # Сохранение в CSV
    df.to_csv(output_path, index=False)
    
    # Сохранение метаданных о станциях
    stations_meta = {
        'stations': [
            {
                'id': i,
                'lon': float(stations_lons[i]),
                'lat': float(stations_lats[i])
            } for i in range(num_stations)
        ]
    }
    
    meta_path = Path(output_path).with_suffix('.stations.json')
    with open(meta_path, 'w') as f:
        json.dump(stations_meta, f, indent=2)
    
    print(f"Сгенерировано {num_stations} станций с данными за 365 дней")
    return output_path

def create_region_json(output_path):
    """Создает JSON файл с границами Массачусетса"""
    massachusetts_bounds = {
        "name": "Массачусетс, США",
        "west": -73.5,
        "east": -69.9,
        "south": 41.2,
        "north": 42.9,
        "description": "Штат Массачусетс, США"
    }
    
    with open(output_path, 'w') as f:
        json.dump(massachusetts_bounds, f, indent=2)
    
    return massachusetts_bounds
