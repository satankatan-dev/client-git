import numpy as np
import pandas as pd
import csv
from pathlib import Path

def calculate_stats(csv_path):
    """Вычисляет основную статистику по данным CSV"""
    df = pd.read_csv(csv_path)
    
    precipitation_values = df['precipitation_mm'].values
    
    # Основная статистика
    stats = {
        'min': np.nanmin(precipitation_values),
        'max': np.nanmax(precipitation_values),
        'mean': np.nanmean(precipitation_values),
        'mean_non_zero': np.nanmean(precipitation_values[precipitation_values > 0]),
        'std': np.nanstd(precipitation_values),
        'count_total': len(precipitation_values),
        'count_non_zero': np.sum(precipitation_values > 0),
        'zero_ratio': np.sum(precipitation_values == 0) / len(precipitation_values)
    }
    
    return stats

def calculate_simple_stats(csv_path, column_index=4):
    """Эффективный по памяти расчет статистики (построчное чтение)"""
    min_val = float('inf')
    max_val = float('-inf')
    total_sum = 0.0
    total_sum_non_zero = 0.0
    count = 0
    count_non_zero = 0
    
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Пропускаем заголовок
        
        for row in reader:
            try:
                val = float(row[column_index])
                
                if val < min_val:
                    min_val = val
                if val > max_val:
                    max_val = val
                
                total_sum += val
                count += 1
                
                if val > 0:
                    total_sum_non_zero += val
                    count_non_zero += 1
                    
            except (ValueError, IndexError):
                continue
    
    stats = {
        'min': min_val,
        'max': max_val,
        'mean': total_sum / count if count > 0 else 0,
        'mean_non_zero': total_sum_non_zero / count_non_zero if count_non_zero > 0 else 0,
        'count_total': count,
        'count_non_zero': count_non_zero
    }
    
    return stats
