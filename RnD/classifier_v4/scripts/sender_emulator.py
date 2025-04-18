import asyncio
import numpy as np
import json


GLOBAL_JSON_CLIENTS_LIST = []


async def server_handler(reader, writer):
    global GLOBAL_JSON_CLIENTS_LIST
    GLOBAL_JSON_CLIENTS_LIST.append((reader, writer))
    print(f'client {len(GLOBAL_JSON_CLIENTS_LIST)} connected to data server')


async def data_server(outcome_socket_path):
    server = await asyncio.start_unix_server(server_handler, outcome_socket_path)
    async with server:
        print('data server started')
        await server.serve_forever()


def make_data_batch():
    delimiter = bytearray(b'<<<EndOfData.>>>')
    das_json_dict = {"ADC_id": 0,
                     "chnTypeId": "t",
                     "correctOrder": True,
                     "devName": "DAS",
                     "impulseNs": 80,
                     "meterOnChn": 2,
                     "numTraces": 1024,
                     "scaleMax": 16383,
                     "scaleMin": 0,
                     "scanMs": 1,
                     "traceBegin": 0,
                     "traceSize": 1200}

    data_npy = np.load("/home/drozdovmk/Projects/ZB/tests/test_data.npy", mmap_mode='r')
    # data_npy = np.load('D:\signals\numpy_kupavna051224\f8_kupavna051224.npy', mmap_mode='r')[:, 100:]
    index = 0
    time_indent = 1024

    das_json_dict['numTraces'] = time_indent
    das_json_dict['traceSize'] = data_npy.shape[1]
    json_data = bytearray(json.dumps(das_json_dict, indent=4).encode()) + b'\0'

    while True:
        if index < round(data_npy.shape[0] / time_indent - 1):
            data_slice = data_npy[time_indent * index:time_indent * index + time_indent,:]
            data = bytearray(json_data + data_slice.tobytes()) + delimiter
            yield data
            print(f'{index}, {len(data)}')
            index += 1
        else:
            index = 0


async def send_data():
    data_gen = make_data_batch()

    while True:
        if len(GLOBAL_JSON_CLIENTS_LIST) > 0:
            data = next(data_gen)

            for reader, writer in GLOBAL_JSON_CLIENTS_LIST:
                try:
                    writer.write(data)
                    await writer.drain()
                except Exception:
                    GLOBAL_JSON_CLIENTS_LIST.remove((reader, writer))
                    print('client disconnected', len(GLOBAL_JSON_CLIENTS_LIST))

        await asyncio.sleep(1)


async def main():
    outcome_socket_path = '/tmp/das_driver'

    await asyncio.gather(data_server(outcome_socket_path), send_data())


if __name__ == "__main__":
    asyncio.run(main())
