import asyncio
import json
import os
import sys
import numpy as np

one_second = 1024 
delimiter = bytearray(b'<<<EndOfData.>>>')
data_socket_path = '/tmp/das_driver'

async def connect_to_data_server(data_socket_path):
    dots = '.'
    while True:
        try:
            reader, writer = await asyncio.open_unix_connection(data_socket_path)
            print('das driver connected')
            return reader, writer
        
        except Exception:
            print('', end="\rconnecting to das driver" + dots)
            await asyncio.sleep(3)
            print('\x1b[2K', end='\r')  # clear line
            if dots == '...':
                dots = '.'
            else:
                dots += '.'


async def data_receiver(data_socket_path):
    # global delimeter
    # await asyncio.sleep(2) 
    buffer = bytearray()
    data_window = np.array([])

    with open('das_config.json') as json_file:
        config_dict = json.load(json_file)

    reader, _ = await connect_to_data_server(data_socket_path)

    while True:
        try:
            data = await reader.readuntil(separator=delimiter)
            buffer += data

            delimiterJSON = buffer.index(b'\0')
            jsonHdrBytes = buffer[:delimiterJSON]
            jsonHdrObj = json.loads(jsonHdrBytes.decode())
            numTraces = jsonHdrObj['numTraces']
            traceSize = jsonHdrObj['traceSize']

            byteData = buffer[delimiterJSON + 1: delimiterJSON + numTraces * traceSize * 2 + 1]
            data_np = np.frombuffer(byteData, dtype=np.uint16).reshape(numTraces, traceSize)


            data_window = np.vstack((data_window, data_np)) if data_window.size else data_np
            if data_window.shape[0] >= config_dict['time_window'] + one_second:
                data_window = np.roll(data_window, shift=-one_second, axis=0)
                data_window = np.delete(data_window, np.s_[-one_second:], axis=0)

            buffer.clear()

        except asyncio.exceptions.LimitOverrunError as e:
            buffer += await reader.read(e.consumed)
        except asyncio.exceptions.IncompleteReadError:
            buffer.clear()
            reader, _ = await connect_to_data_server(data_socket_path)


async def get_data():
    data_socket_path = '/tmp/das_driver'  # '/tmp/ioes_opal0a0'
    await asyncio.gather(data_receiver(data_socket_path))

if __name__ == "__main__":
    asyncio.run(get_data())
