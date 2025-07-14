
import asyncio
import numpy as np
import json

class DataReceiver():
    def __init__(self, socket_path):
        self.socket_path = socket_path
        self.driver_one_second = 1024 # настройка для драйвера
        self.driver_delimiter = bytearray(b'<<<EndOfData.>>>') # разделитель конца сообщения
        self.event = asyncio.Event()
        self.connect_to_data_server()
        
    async def get_data(self):
        try:
            data = await self.reader.readuntil(separator=self.driver_delimiter)
            buffer += data

            delimiterJSON = buffer.index(b'\0')
            jsonHdrBytes = buffer[:delimiterJSON]
            jsonHdrObj = json.loads(jsonHdrBytes.decode())
            numTraces = jsonHdrObj['numTraces'] # кол-во рефлектограм
            traceSize = jsonHdrObj['traceSize'] # длина рефлекторгам

            byteData = buffer[delimiterJSON + 1: delimiterJSON + numTraces * traceSize * 2 + 1]
            data_np = np.frombuffer(byteData, dtype=np.uint16).reshape(numTraces, traceSize)


            self.data_window = np.vstack((self.data_window, data_np)) if self.data_window.size else data_np
            if self.data_window.shape[0] >= self.driver_one_second + self.driver_one_second:
                self.data_window = self.data_window[(self.data_window.shape[0]-self.driver_one_second):]
            buffer.clear()
            # здесь нужно придумать как вытащить из класса data_window
            # моя идея: заблокироваться через примитив синхронизации, потом 
            # взять data_window и продолжить выполнение сняв примитив синхронизации
            self.event.set()

        except asyncio.exceptions.LimitOverrunError as e:
            buffer += await self.reader.read(e.consumed)
        except asyncio.exceptions.IncompleteReadError:
            buffer.clear()
            self.reader, _ = await self.connect_to_data_server()
        
    async def connect_to_data_server(self):
        dots = '.'
        while True:
            try:
                self.reader, self.writer = await asyncio.open_unix_connection(self.data_socket_path)
                print('das driver connected')
            
            except Exception:
                print('', end="\rconnecting to das driver" + dots)
                await asyncio.sleep(3)
                print('\x1b[2K', end='\r')  # clear line
                if dots == '...':
                    dots = '.'
                else:
                    dots += '.'
