import argparse
import json
import numpy as np
import rasterio
from pathlib import Path
import time
from batch_manager import BatchManager
from polygon_utils import load_geojson_polygon, create_polygon_mask
from shared import interpolation_core, data_generator

def main():
    parser = argparse.ArgumentParser(
        description="Клиент для распределенной интерполяции осадков"
    )
    
    parser.add_argument("--region-json", required=True, help="JSON файл с границами региона")
    parser.add_argument("--output-tif", required=True, help="Путь для сохранения GeoTIFF")
    parser.add_argument("--stations", type=int, default=80, help="Количество станций")
    parser.add_argument("--resolution", type=float, default=0.01, help="Разрешение растра в градусах")
    parser.add_argument("--power", type=float, default=2.0, help="Степень для IDW интерполяции")
    parser.add_argument("--polygon-geojson", help="GeoJSON с ограничивающим полигоном")
    parser.add_argument("--servers", nargs='+', required=True, 
                       help="URL серверов, например: http://192.168.1.100:5000")
    parser.add_argument("--batch-size", type=int, default=20, help="Количество строк в батче")
    
    args = parser.parse_args()
    
    print("🚀 КЛИЕНТ ДЛЯ РАСПРЕДЕЛЕННЫХ ВЫЧИСЛЕНИЙ")
    print("=" * 50)
    print(f"Серверы: {args.servers}")
    print(f"Размер батча: {args.batch_size} строк")
    start_time = time.time()
    
    # Загрузка региона
    try:
        with open(args.region_json, 'r') as f:
            region_bounds = json.load(f)
        print(f"✓ Регион загружен: {region_bounds['name']}")
    except Exception as e:
        print(f"✗ Ошибка загрузки региона: {e}")
        return
    
    # Генерация данных
    csv_path = "massachusetts_precipitation_data.csv"
    try:
        print("📊 Генерация данных...")
        data_generator.generate_precipitation_data(region_bounds, args.stations, csv_path)
        print(f"✓ Данные сгенерированы: {csv_path}")
    except Exception as e:
        print(f"✗ Ошибка генерации данных: {e}")
        return
    
    # Загрузка известных данных
    try:
        known_data = interpolation_core.load_known_data(csv_path)
        print(f"✓ Загружено {len(known_data['lons'])} станций")
    except Exception as e:
        print(f"✗ Ошибка загрузки данных станций: {e}")
        return
    
    # Создание сетки
    try:
        lons_grid, lats_grid, transform = interpolation_core.create_grid(
            region_bounds, args.resolution
        )
        height, width = lons_grid.shape
        print(f"✓ Создана сетка: {height} x {width} пикселей")
    except Exception as e:
        print(f"✗ Ошибка создания сетки: {e}")
        return
    
    # Загрузка полигона (если указан)
    polygon_mask = None
    if args.polygon_geojson:
        try:
            print("🔷 Загрузка ограничивающего полигона...")
            polygon = load_geojson_polygon(args.polygon_geojson)
            polygon_mask = create_polygon_mask(polygon, transform, width, height)
            pixels_inside = np.sum(polygon_mask)
            print(f"✓ Полигон загружен: {pixels_inside} пикселей внутри полигона ({pixels_inside/(height*width)*100:.1f}%)")
        except Exception as e:
            print(f"✗ Ошибка загрузки полигона: {e}")
            return
    
    # Создание батчей
    print("📦 Подготовка батчей...")
    batches = []
    for start_row in range(0, height, args.batch_size):
        end_row = min(start_row + args.batch_size, height)
        
        batch_data = {
            'start_row': start_row,
            'end_row': end_row,
            'lons_grid': lons_grid[start_row:end_row],
            'lats_grid': lats_grid[start_row:end_row],
            'known_data': known_data,
            'power': args.power,
            'polygon_mask': polygon_mask[start_row:end_row] if polygon_mask is not None else None
        }
        batches.append(batch_data)
    
    print(f"✓ Создано {len(batches)} батчей")
    
    # Распределение батчей по серверам
    print("🌐 Распределение вычислений...")
    try:
        batch_manager = BatchManager(args.servers)
        results = batch_manager.distribute_batches(batches, max_workers=len(args.servers))
        
        if not results:
            print("✗ Не получено ни одного результата от серверов")
            return
            
        print(f"✓ Получено результатов: {len(results)}/{len(batches)} батчей")
        
    except Exception as e:
        print(f"✗ Ошибка распределения батчей: {e}")
        return
    
    # Сбор результатов
    print("🔄 Сбор результатов...")
    result_array = np.full((height, width, 2), np.nan, dtype=np.float32)
    
    successful_batches = 0
    for start_row, batch_results in results.items():
        if batch_results is not None:
            batch_size = batch_results.shape[0]
            result_array[start_row:start_row + batch_size] = batch_results
            successful_batches += 1
    
    print(f"✓ Собрано {successful_batches} батчей")
    
    # Сохранение результатов
    print("💾 Сохранение результатов...")
    try:
        output_path = Path(args.output_tif)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        profile = {
            'driver': 'GTiff',
            'height': height,
            'width': width,
            'count': 2,
            'dtype': np.float32,
            'crs': 'EPSG:4326',
            'transform': transform,
            'compress': 'lzw',
            'nodata': np.nan
        }
        
        with rasterio.open(str(output_path), 'w', **profile) as dst:
            dst.write(result_array[:, :, 0], 1)
            dst.write(result_array[:, :, 1], 2)
            dst.set_band_description(1, "Maximum precipitation")
            dst.set_band_description(2, "Mean precipitation (non-zero)")
        
        print(f"✓ Результаты сохранены: {output_path}")
        
    except Exception as e:
        print(f"✗ Ошибка сохранения: {e}")
        return
    
    # Статистика выполнения
    total_time = time.time() - start_time
    print("=" * 50)
    print("✅ ВЫЧИСЛЕНИЯ ЗАВЕРШЕНЫ УСПЕШНО!")
    print(f"⏱️  Общее время: {total_time:.1f} секунд")
    print(f"📊 Успешных батчей: {successful_batches}/{len(batches)}")
    print(f"🌐 Использовано серверов: {len(args.servers)}")

if __name__ == '__main__':
    main()