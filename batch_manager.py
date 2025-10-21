import requests
import numpy as np
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class BatchManager:
    def __init__(self, server_urls):
        self.server_urls = server_urls
        self.current_server = 0
        self.completed_batches = 0
        self.total_batches = 0
    
    def get_next_server(self):
        """Round-robin распределение по серверам"""
        server = self.server_urls[self.current_server]
        self.current_server = (self.current_server + 1) % len(self.server_urls)
        return server
    
    def check_server_health(self, server_url):
        """Проверяет доступность сервера"""
        try:
            response = requests.get(f"{server_url}/health", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def send_batch_to_server(self, batch_data, server_url):
        """Отправляет батч на сервер и получает результат"""
        try:
            # Подготовка данных для JSON сериализации
            serializable_data = {
                'start_row': batch_data['start_row'],
                'end_row': batch_data['end_row'],
                'lons_grid': batch_data['lons_grid'].tolist(),
                'lats_grid': batch_data['lats_grid'].tolist(), 
                'known_data': {
                    'lons': batch_data['known_data']['lons'].tolist(),
                    'lats': batch_data['known_data']['lats'].tolist(),
                    'max_values': batch_data['known_data']['max_values'].tolist(),
                    'mean_values': batch_data['known_data']['mean_values'].tolist()
                },
                'power': batch_data['power'],
                'polygon_mask': batch_data.get('polygon_mask', None)
            }
            
            if serializable_data['polygon_mask'] is not None:
                serializable_data['polygon_mask'] = serializable_data['polygon_mask'].tolist()
            
            # Отправка запроса
            start_time = time.time()
            response = requests.post(
                f"{server_url}/process_batch",
                json=serializable_data,
                timeout=3600  # 1 час таймаут
            )
            processing_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                self.completed_batches += 1
                print(f"✅ [{self.completed_batches}/{self.total_batches}] {server_url}: строки {batch_data['start_row']}-{batch_data['end_row']} ({processing_time:.1f}с)")
                
                # Конвертируем результат обратно в numpy
                results_array = np.array(result['results'], dtype=np.float32)
                return result['start_row'], results_array
            else:
                print(f"❌ {server_url}: Ошибка {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            print(f"⏰ {server_url}: Таймаут при обработке батча")
            return None
        except requests.exceptions.ConnectionError:
            print(f"🔌 {server_url}: Ошибка соединения")
            return None
        except Exception as e:
            print(f"💥 {server_url}: Неожиданная ошибка: {e}")
            return None
    
    def distribute_batches(self, batches_data, max_workers=None):
        """Распределяет батчи по серверам и собирает результаты"""
        self.total_batches = len(batches_data)
        self.completed_batches = 0
        
        print(f"🔍 Проверка доступности серверов...")
        available_servers = []
        for server_url in self.server_urls:
            if self.check_server_health(server_url):
                available_servers.append(server_url)
                print(f"   ✓ {server_url} - доступен")
            else:
                print(f"   ✗ {server_url} - недоступен")
        
        if not available_servers:
            print("❌ Нет доступных серверов!")
            return {}
        
        self.server_urls = available_servers
        print(f"🌐 Используется {len(available_servers)} серверов")
        
        results = {}
        
        # Распределение батчей
        with ThreadPoolExecutor(max_workers=max_workers or len(available_servers)) as executor:
            future_to_batch = {}
            
            for batch_data in batches_data:
                server_url = self.get_next_server()
                future = executor.submit(self.send_batch_to_server, batch_data, server_url)
                future_to_batch[future] = batch_data['start_row']
            
            # Сбор результатов
            for future in as_completed(future_to_batch):
                start_row = future_to_batch[future]
                try:
                    result = future.result()
                    if result:
                        results[start_row] = result[1]
                except Exception as e:
                    print(f"💥 Исключение для строк {start_row}: {e}")
        
        return results