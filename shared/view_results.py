import rasterio
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def analyze_geotiff(file_path):
    """Анализ и визуализация GeoTIFF файла"""
    
    if not Path(file_path).exists():
        print(f"Файл {file_path} не найден!")
        return
    
    with rasterio.open(file_path) as src:
        # Основная информация
        print("=" * 50)
        print("АНАЛИЗ GEO TIFF ФАЙЛА")
        print("=" * 50)
        print(f"Файл: {file_path}")
        print(f"Размер: {src.width} x {src.height} пикселей")
        print(f"Количество каналов: {src.count}")
        print(f"Границы: {src.bounds}")
        print(f"Разрешение: {src.res}")
        print(f"Система координат: {src.crs}")
        
        # Чтение данных
        data = src.read()
        
        # Создание подписей для каналов
        band_names = ["Максимальные осадки", "Средние осадки (ненулевые)"]
        
        # Визуализация каждого канала
        fig, axes = plt.subplots(1, src.count, figsize=(15, 5))
        if src.count == 1:
            axes = [axes]
        
        for i in range(src.count):
            band_data = data[i]
            
            # Статистика
            valid_data = band_data[~np.isnan(band_data)]
            if len(valid_data) > 0:
                print(f"\n--- Канал {i+1}: {band_names[i]} ---")
                print(f"   Min: {np.min(valid_data):.2f} мм")
                print(f"   Max: {np.max(valid_data):.2f} мм")
                print(f"   Mean: {np.mean(valid_data):.2f} мм")
                print(f"   Std: {np.std(valid_data):.2f} мм")
            
            # Визуализация
            im = axes[i].imshow(band_data, cmap='viridis')
            axes[i].set_title(f'{band_names[i]}\n(мм/сутки)')
            plt.colorbar(im, ax=axes[i], label='мм')
        
        plt.tight_layout()
        plt.savefig('geotiff_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # Сохранение статистики в файл
        with open('geotiff_statistics.txt', 'w') as f:
            f.write("СТАТИСТИЧЕСКИЙ АНАЛИЗ РЕЗУЛЬТАТОВ\n")
            f.write("=" * 40 + "\n")
            for i in range(src.count):
                band_data = data[i]
                valid_data = band_data[~np.isnan(band_data)]
                if len(valid_data) > 0:
                    f.write(f"\nКанал {i+1}: {band_names[i]}\n")
                    f.write(f"  Min: {np.min(valid_data):.2f} мм\n")
                    f.write(f"  Max: {np.max(valid_data):.2f} мм\n")
                    f.write(f"  Mean: {np.mean(valid_data):.2f} мм\n")
                    f.write(f"  Std: {np.std(valid_data):.2f} мм\n")
        
        print(f"\nСтатистика сохранена в 'geotiff_statistics.txt'")
        print(f"Графики сохранены в 'geotiff_analysis.png'")

if __name__ == "__main__":
    analyze_geotiff('results/massachusetts_precipitation.tif')
