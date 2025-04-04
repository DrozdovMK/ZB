from data_client import connect_to_data_server
import json
import asyncio
import numpy as np
import sys


class Starter():
    def __init__(self,
                driver_config_path="configs/das_config.json",
                zone_config_path="configs/zone_config.json"):
        
        with open(driver_config_path) as json_file:
            self.driver_config = json.load(json_file) # конфигурационный файл с настройками драйвера
        with open(zone_config_path) as json_file:
            self.zone_config = json.load(json_file) # конфигурационный файл со всеми зонами
            print(self.zone_config)
        
        self.driver_one_second = 1024 # кол-во отсчетов присылаемое за раз
        self._driver_delimiter = bytearray(b'<<<EndOfData.>>>') # разделитель конца сообщения
        self._data_socket_path = "/tmp/das_driver" # имя UNIX сокета
        
    
    async def start(self):
        reader, _ = await connect_to_data_server(self._data_socket_path)
        buffer = bytearray()
        reflectogram = np.array([])
        processes = []
        
        # Запускаем процессы для каждой зоны
        for zone_config in self.zone_config:
            print(f"Запуск процесса для зоны {zone_config['zone_num']}")
            process = await asyncio.create_subprocess_exec(sys.executable,
                                                          "mainloop_async2.py",
                                                          json.dumps(zone_config),
                                                          stdin=asyncio.subprocess.PIPE,
                                                          stdout=asyncio.subprocess.PIPE,
                                                          stderr=asyncio.subprocess.PIPE)
            processes.append((zone_config["zone_num"], process))
        
        while True:
            try:
                data = await reader.readuntil(separator=self._driver_delimiter)
                buffer += data
                delimiterJSON = buffer.index(b'\0')
                jsonHdrBytes = buffer[:delimiterJSON]
                jsonHdrObj = json.loads(jsonHdrBytes.decode())
                numTraces = jsonHdrObj['numTraces']
                traceSize = jsonHdrObj['traceSize']
                byteData = buffer[delimiterJSON + 1: delimiterJSON + numTraces * traceSize * 2 + 1]
                data_np = np.frombuffer(byteData, dtype=np.uint16).reshape(numTraces, traceSize)
                reflectogram = np.vstack((reflectogram, data_np)) if reflectogram.size else data_np
                
                if reflectogram.shape[0] >= self.driver_config['time_window'] + self.driver_one_second:
                    reflectogram = np.roll(reflectogram, shift = -self.driver_one_second, axis=0)
                    reflectogram = np.delete(reflectogram, np.s_[-self.driver_one_second:], axis=0)
                    print(f"Reflectogram received: {reflectogram.shape}")
                    
                    # Отправляем данные каждому процессу
                    for zone_num, process in processes:
                        # Проверяем, что процесс еще работает
                        if process.returncode is None:
                            try:
                                # Отправляем данные в процесс
                                process.stdin.write(reflectogram[:, zone_num].tobytes())
                                await process.stdin.drain()
                            except Exception as e:
                                print(f"Ошибка при отправке данных процессу зоны {zone_num}: {e}")
                    
                    # Получаем результаты от каждого процесса (неблокирующий способ)
                    for zone_num, process in processes:
                        if process.returncode is None:
                            try:
                                # Проверяем, доступны ли данные для чтения
                                line = await asyncio.wait_for(process.stdout.readline(), timeout=0.1)
                                if line:
                                    print(f"Зона {zone_num}, ответ: {line.decode().strip()}")
                            except asyncio.TimeoutError:
                                # Нет данных для чтения - продолжаем
                                pass
                            except Exception as e:
                                print(f"Ошибка при чтении данных от процесса зоны {zone_num}: {e}")
                buffer.clear()
            except asyncio.exceptions.LimitOverrunError as e:
                buffer += await reader.read(e.consumed)
            except asyncio.exceptions.IncompleteReadError:
                buffer.clear()
                print("Переподключение к серверу данных...")
                reader, _ = await connect_to_data_server(self._data_socket_path)
            except Exception as e:
                print(f"Неожиданная ошибка: {e}")
                # Добавляем небольшую задержку, чтобы избежать цикла с быстрыми ошибками
                await asyncio.sleep(1)



if __name__ == "__main__":
    try:
        s = Starter(driver_config_path="configs/das_config.json", 
                    zone_config_path="configs/zone_config.json")
        asyncio.run(s.start())
    except KeyboardInterrupt:
        print("Программа остановлена пользователем")
