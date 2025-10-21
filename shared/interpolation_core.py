import numpy as np
import pandas as pd
import pyproj
from rasterio.transform import from_origin

def load_known_data(csv_path):
    """Загружает данные известных точек из CSV файла"""
    df = pd.read_csv(csv_path)
    
    # Группировка по станциям и расчет статистики
    station_stats = df.groupby(['station_id', 'longitude', 'latitude']).agg({
        'precipitation_mm': ['max', lambda x: np.nanmean(x[x > 0])]
    }).reset_index()
    
    station_stats.columns = ['station_id', 'longitude', 'latitude', 'max_precip', 'mean_precip_nonzero']
    
    known_data = {
        'lons': station_stats['longitude'].values,
        'lats': station_stats['latitude'].values,
        'max_values': station_stats['max_precip'].values,
        'mean_values': station_stats['mean_precip_nonzero'].values
    }
    
    return known_data

def create_grid(region_bounds, resolution):
    """Создает координатную сетку для региона"""
    west = region_bounds['west']
    east = region_bounds['east']
    south = region_bounds['south']
    north = region_bounds['north']
    
    # Расчет размеров растра
    width = int((east - west) / resolution) + 1
    height = int((north - south) / resolution) + 1
    
    # Создание сетки координат
    lons = np.linspace(west, east, width)
    lats = np.linspace(south, north, height)
    
    lons_grid, lats_grid = np.meshgrid(lons, lats)
    
    # Создание трансформации для GeoTIFF
    transform = from_origin(west, north, resolution, resolution)
    
    return lons_grid, lats_grid, transform

def calculate_distances_geod(target_lon, target_lat, known_lons, known_lats):
    """Векторизованный расчет расстояний с использованием pyproj.Geod"""
    geod = pyproj.Geod(ellps='WGS84')
    
    n_known = len(known_lons)
    target_lons_repeated = np.repeat(target_lon, n_known)
    target_lats_repeated = np.repeat(target_lat, n_known)
    
    _, _, distances = geod.inv(
        target_lons_repeated,
        target_lats_repeated,
        known_lons,
        known_lats
    )
    
    return distances

def idw_interpolation(target_lon, target_lat, known_data, power=2.0):
    """IDW интерполяция для одной точки"""
    distances = calculate_distances_geod(target_lon, target_lat, 
                                       known_data['lons'], known_data['lats'])
    
    # Избегаем деления на ноль
    distances = np.maximum(distances, 1e-9)
    
    # Расчет весов
    weights = 1.0 / (distances ** power)
    weights_sum = np.sum(weights)
    
    if weights_sum > 0:
        max_interpolated = np.sum(weights * known_data['max_values']) / weights_sum
        mean_interpolated = np.sum(weights * known_data['mean_values']) / weights_sum
        
        # Защита от nan
        if np.isnan(max_interpolated):
            max_interpolated = 0.0
        if np.isnan(mean_interpolated):
            mean_interpolated = 0.0
            
        return max_interpolated, mean_interpolated
    else:
        return 0.0, 0.0  # Вместо nan возвращаем 0
