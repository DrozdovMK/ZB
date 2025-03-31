import asyncio
import json


async def connect_to_server(host, port):
    dots = '.'
    while True:
        try:
            # reader, writer = await asyncio.open_unix_connection(income_socket_path)
            reader, writer = await asyncio.open_connection(host, port)
            print('json socket connected')
            return reader, writer
            break
        except Exception:
            print('', end="\rconnecting to json socket" + dots)
            await asyncio.sleep(1)
            print('\x1b[2K', end='\r')  # clear line
            if dots == '...':
                dots = '.'
            else:
                dots += '.'


async def data_receiver(host, port):
    await asyncio.sleep(1)
    delimiter = bytearray(b'<<<EndOfData.>>>')
    buffer = bytearray()

    reader, writer = await connect_to_server(host, port)

    while True:
        try:
            data = await reader.readuntil(separator=delimiter)
            buffer += data

            delimiterJSON = buffer.index(delimiter)
            jsonHdrBytes = buffer[:delimiterJSON]
            jsonHdrObj = json.loads(jsonHdrBytes.decode())
            print(jsonHdrObj)

            # for item in jsonHdrObj['perimeter_intrusion']:
            #    print(json.dumps(item, indent=4))

            buffer.clear()

        except asyncio.exceptions.LimitOverrunError as e:
            buffer += await reader.read(e.consumed)
        except asyncio.exceptions.IncompleteReadError:
            buffer.clear()
            reader, writer = await connect_to_server(host, port)


async def main():
    # json_socket_path = '/tmp/lcdas_json'
    host = '127.0.0.1'
    port = 9999

    await asyncio.gather(data_receiver(host, port))


if __name__ == "__main__":
    asyncio.run(main())
