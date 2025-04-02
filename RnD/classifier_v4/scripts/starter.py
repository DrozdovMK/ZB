from data_client import connect_to_data_server
import json
import asyncio
import numpy as np
import sys


class Starter():
    def __init__(self,
                driver_config_path="configs/das_config.json",
                zone_config_path="configs/zone_configs.json"):
        
        with open(driver_config_path) as json_file:
            self.driver_config = json.load(json_file) # конфигурационный файл с настройками драйвера
        with open(zone_config_path) as json_file:
            self.zone_config = json.load(json_file) # конфигурационный файл со всеми зонами
        
        self.driver_one_second = 1024 # кол-во отсчетов присылаемое за раз
        self._driver_delimiter = bytearray(b'<<<EndOfData.>>>') # разделитель конца сообщения
        self._data_socket_path = "/tmp/das_driver" # имя UNIX сокета
        
    
    async def start(self):
        reader, _ = await connect_to_data_server(self._data_socket_path)
        buffer = bytearray()
        reflectogram = np.array([])
        processes = []
        
        for zone_config in self.zone_config:
            process = await asyncio.create_subprocess_exec(sys.executable,
                                                            "mainloop_async2.py",
                                                            zone_config,
                                                            stdin=asyncio.subprocess.PIPE,
                                                            stdout=asyncio.subprocess.PIPE)
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
                        print(reflectogram.shape)
                        for (zone_num, process) in processes:
                            _ = await process.communicate(reflectogram[:, zone_num].tobytes())
                        
                        for (zone_num, process) in processes:
                            data = await process.communicate()
                            print(zone_num, data)
                            sys.stdout.flush()
                    buffer.clear()
                except asyncio.exceptions.LimitOverrunError as e:
                    buffer += await reader.read(e.consumed)
                except asyncio.exceptions.IncompleteReadError:
                    buffer.clear()
                    reader, _ = await connect_to_data_server(self._data_socket_path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("you must specify zone number")
    else:
        s = Starter(driver_config_path="configs/das_config.json", 
                    zone_config_path = "configs/zone_configs.json")
        asyncio.run(s.start())
