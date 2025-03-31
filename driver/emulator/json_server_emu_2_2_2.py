import asyncio
import time
import json
from random import randint

GLOBAL_JSON_CLIENTS_LIST = []
GLOBAL_ACTUAL_ALARMS_LIST = []
delimiter = bytearray(b'<<<EndOfData.>>>')


async def json_send(data_in):
    global GLOBAL_JSON_CLIENTS_LIST
    global delimiter
    json_data = bytearray(json.dumps(data_in).encode()) + delimiter
    for writer in GLOBAL_JSON_CLIENTS_LIST:
        try:
            writer.write(json_data)
            await writer.drain()
        except Exception as msg:
            print('JSON send error:', msg)
            GLOBAL_JSON_CLIENTS_LIST.remove(writer)
            print('client disconnected, connected clients:', len(GLOBAL_JSON_CLIENTS_LIST))


async def server_handler(reader, writer):
    global GLOBAL_JSON_CLIENTS_LIST
    global GLOBAL_ACTUAL_ALARMS_LIST
    delimiter = bytearray(b'<<<EndOfData.>>>')
    das_json_dict = {}
    das_json_dict['actual_alarms'] = GLOBAL_ACTUAL_ALARMS_LIST
    json_data = bytearray(json.dumps(das_json_dict).encode()) + delimiter
    writer.write(json_data)
    await writer.drain()
    GLOBAL_JSON_CLIENTS_LIST.append(writer)
    print(f'client {len(GLOBAL_JSON_CLIENTS_LIST)} connected to das_json')


async def json_server(host, port):
    # server = await asyncio.start_unix_server(server_handler, outcome_socket_path)
    server = await asyncio.start_server(server_handler, host, port)
    async with server:
        print('json server started')
        await server.serve_forever()


def make_alerts():
    iter = 0

    alarms_list = [{
        'alarmType': 'new',
        'channel': 100,
        'comment': 'Someone is walking',
        'id': 0,
        'intrusionType': 'human',
        'severity': 'alarm',
        'timestamp': round(time.time()),
        'width': randint(10, 20),
        'meterPerChannel': 2
    }, {
        'alarmType': 'new',
        'channel': 200,
        'comment': 'Group is walking',
        'id': 1,
        'intrusionType': 'group',
        'severity': 'alarm',
        'timestamp': round(time.time()),
        'width': randint(20, 30),
        'meterPerChannel': 2
    }, {
        'alarmType': 'new',
        'channel': 300,
        'comment': 'Car is moving',
        'id': 2,
        'intrusionType': 'car',
        'severity': 'alarm',
        'timestamp': round(time.time()),
        'width': randint(30, 40),
        'meterPerChannel': 2
    }]

    del_alarms_list = [{
        'alarmType': 'delete',
        'id': 0,
        'intrusionType': 'human',
        'timestamp': round(time.time())
    }, {
        'alarmType': 'delete',
        'id': 1,
        'intrusionType': 'group',
        'timestamp': round(time.time())
    }, {
        'alarmType': 'delete',
        'id': 2,
        'intrusionType': 'car',
        'timestamp': round(time.time())
    }]

    while True:

        if 0 <= iter < 40:
            yield alarms_list
            iter += 1
        # elif 30 <= iter < 40:
            # iter += 1
            # time.sleep(1)
            # yield []
        else:
            iter = 0
            yield del_alarms_list
            for del_alarm in del_alarms_list:
                del_alarm['id'] += 3

            for alarm in alarms_list:
                alarm['id'] += 3

        for alarm in alarms_list:
            alarm['alarmType'] = 'new' if iter == 0 else 'old'

            if iter > 0:
                if alarm['intrusionType'] == 'human':
                    alarm['channel'] += 2
                elif alarm['intrusionType'] == 'group':
                    alarm['channel'] += 3
                else:
                    alarm['channel'] += 5
            else:
                if alarm['intrusionType'] == 'human':
                    alarm['channel'] = 100
                elif alarm['intrusionType'] == 'group':
                    alarm['channel'] = 200
                else:
                    alarm['channel'] = 300

            alarm['timestamp'] = round(time.time())

            if alarm['intrusionType'] == 'human':
                alarm['width'] = randint(10, 20)
            elif alarm['intrusionType'] == 'group':
                alarm['width'] = randint(20, 30)
            else:
                alarm['width'] = randint(30, 40)


async def send_alarms():
    global GLOBAL_JSON_CLIENTS_LIST
    global GLOBAL_ACTUAL_ALARMS_LIST
    das_json_dict = {}
    alarms_list_gen = make_alerts()
    counter = 20
    await asyncio.sleep(3)

    while True:
        if 0 < counter < 5:
            await json_send({"ERROR": [{"error_string": "DAS ethernet communication breakdown. Reconnecting...", "errno": 4}]})
            counter -= 1
        elif 10 < counter < 15:
            await json_send({"ERROR": [{"error_string": "no errors", "errno": 0}]})
            counter -= 1
        elif counter == 0:
            counter = 20
        else:
            alarms_list = next(alarms_list_gen)
            arch_id_lst = [alarm['id'] for alarm in GLOBAL_ACTUAL_ALARMS_LIST]
            for item in alarms_list:
                if item['alarmType'] != 'delete':
                    if item['id'] not in arch_id_lst:
                        GLOBAL_ACTUAL_ALARMS_LIST.append(item.copy())
                    else:
                        GLOBAL_ACTUAL_ALARMS_LIST[arch_id_lst.index(item['id'])] = item.copy()
                else:
                    del GLOBAL_ACTUAL_ALARMS_LIST[arch_id_lst.index(item['id'])]
                    arch_id_lst = [alarm['id'] for alarm in GLOBAL_ACTUAL_ALARMS_LIST]

            das_json_dict['perimeter_intrusion'] = alarms_list
            await json_send(das_json_dict)
            await json_send({"ERROR": [{"error_string": "no errors", "errno": 0}]})
            counter -= 1

        print('alarms generated')
        await asyncio.sleep(1)


async def main():
    host = '127.0.0.1'
    port = 9999
    # outcome_socket_path = '/tmp/das_json'

    await asyncio.gather(json_server(host, port), send_alarms())


if __name__ == "__main__":
    asyncio.run(main())
