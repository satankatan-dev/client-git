import json
import numpy as np
from rasterio.features import geometry_mask
from shapely.geometry import shape, Point
import rasterio

def load_geojson_polygon(geojson_path):
    """Загружает полигон из GeoJSON файла"""
    with open(geojson_path, 'r') as f:
        geojson_data = json.load(f)
    
    # Поддерживаем разные форматы GeoJSON
    if geojson_data['type'] == 'FeatureCollection':
        polygon_geometry = geojson_data['features'][0]['geometry']
    elif geojson_data['type'] == 'Feature':
        polygon_geometry = geojson_data['geometry']
    else:
        polygon_geometry = geojson_data
    
    return shape(polygon_geometry)

def create_polygon_mask(polygon, transform, width, height):
    """Создает маску для пикселей внутри полигона"""
    mask = geometry_mask(
        [polygon], 
        transform=transform, 
        out_shape=(height, width), 
        invert=True  # invert=True означает, что True - внутри полигона
    )
    return mask

def filter_points_by_polygon(points_lons, points_lats, polygon):
    """Фильтрует точки по полигону"""
    points_inside = []
    for lon, lat in zip(points_lons, points_lats):
        point = Point(lon, lat)
        if polygon.contains(point):
            points_inside.append((lon, lat))
    return np.array(points_inside)

def create_test_polygon(output_path):
    """Создает тестовый полигон для Массачусетса"""
    test_polygon = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"name": "Central Massachusetts Test Area"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-72.2, 42.1],
                        [-71.8, 42.1],
                        [-71.8, 42.4], 
                        [-72.2, 42.4],
                        [-72.2, 42.1]
                    ]]
                }
            }
        ]
    }
    
    with open(output_path, 'w') as f:
        json.dump(test_polygon, f, indent=2)
    
    print(f"✓ Создан тестовый полигон: {output_path}")
    return output_path