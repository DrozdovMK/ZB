import socket
import os
import sys
import numpy as np
import json
import signal
import subprocess
import time
import psutil
import multiprocessing as mp
from threading import Thread
from das_driver_graphics import ADCPlot


sock_msg = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_data = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

data_01 = bytearray(b'\xcc\xdd\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00')
data_02 = bytearray(b'\xcc\xdd\x01\x02\x00\x00\x00\x00\x00\x02\x00\x00')
data_03 = bytearray(b'\xcc\xdd\x01\x03\x00\x00\x96\xf8')
data_04 = bytearray(b'\xcc\xdd\x01\x04\x0f\xff\x20\x00\x00\x00\x00\x00\x0f\xff\x04\x00\x00\x00\x00\x00')


DAS_host = '192.168.180.100'
DAS_hardware = '192.168.180.10'
msg_port = 5001
data_port = 5002


def data_processor(data_socket_path, config_dict):

    if sys.argv[2] == 'send':
        das_json_dict = {
            "ADC_id": 0,
            "chnTypeId": "t",
            "correctOrder": True,
            "devName": "das",
            "impulseNs": 80,
            "meterOnChn": 2,
            "numTraces": 128,
            "scaleMax": 16383,
            "scaleMin": 0,
            "scanMs": 1,
            "traceBegin": 0,
            "traceSize": 4000}

        das_json_dict['numTraces'] = config_dict['numTraces']
        das_json_dict['traceBegin'] = config_dict['capture_start']
        das_json_dict['traceSize'] = config_dict['capture_length']
        das_json_dict['impulseNs'] = config_dict['impulse_width']

        json_data = bytearray(json.dumps(das_json_dict).encode()) + b'\0'

        try:
            os.unlink(data_socket_path)
        except OSError:
            if os.path.exists(data_socket_path):
                raise

        server_data = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_data.bind(data_socket_path)
        server_data.listen(1)
        # accept connections
        print('Data server is listening for incoming connections...')
        connection_data, _ = server_data.accept()
        print('Data client connected')

    def data_send(data_pipe):
        delimiter = b'<<<EndOfData.>>>'
        while True:
            data = data_pipe.recv()
            data[0::2], data[1::2] = data[1::2], data[0::2]
            try:
                connection_data.send(json_data + data + delimiter)
            except Exception as err:
                # print(err)
                print('Data client disconnected, trying to reconnect...')
                connection_data, _ = server_data.accept()
                print('... connected')

    def data_plot(data_pipe):
        ADC_plot = ADCPlot()
        while True:
            data = data_pipe.recv()
            data_np = np.frombuffer(data, dtype='>u2').reshape(config_dict['numTraces'], config_dict['capture_length'])
            ADC_plot.plot(data=data_np[0])

    if sys.argv[2] == 'send':
        return_func = data_send
    elif sys.argv[2] == 'plot':
        return_func = data_plot
    else:
        return_func = None

    return return_func


def stop_das():
    data_01[4:8] = b'\x00\x00\x00\x00'
    while True:
        sock_msg.sendto(data_01, (DAS_hardware, msg_port))
        try:
            data_rec = sock_msg.recv(64)
            if data_rec[2] == 1:
                print('DAS stopped')
                return True
            else:
                sys.exit('failed to stop DAS')
        except Exception as err:
            print('trying to stop DAS...', err)


def send_config(data_in):
    try:
        sock_msg.sendto(data_in, (DAS_hardware, msg_port))
    except Exception:
        print('config not sent')
    try:
        data_rec = sock_msg.recv(64)
    except socket.timeout as err:
        print('DAS reply not recieved')
        try:
            connection_errors.send(b'{"ERROR": [{"error_string": "DAS hardware FPGA configuration failed. Driver stopped", "errno": 3}]}\n')
        except Exception:
            pass
        sys.exit(f'DAS hardware FPGA configuration failed. Driver stopped: {err}')
    return data_rec[2]


def start_das(data_01, data_02, data_03, data_04):
    data_01[4:8] = b'\x00\x00\x00\x01'

    # st4 = send_config(data_04)
    # print(st4)

    st3 = send_config(data_03)
    # print(st3)

    st2 = send_config(data_02)
    # print(st2)

    st1 = send_config(data_01)
    # print(st1)
    if st1 == 1 and st2 == 1 and st3 == 1:  # and st4 == 1:
        print('DAS config done')
        return True
    else:
        return False


def exit_driver(signum, frame):
    data_01[4:8] = b'\x00\x00\x00\x00'
    sock_msg.sendto(data_01, (DAS_hardware, msg_port))
    try:
        data_rec = sock_msg.recv(64)
        if data_rec[2] == 1:
            sys.exit('\nDAS stopped')
        else:
            sys.exit('\nfailed to stop DAS')
    except Exception as err:
        sys.exit(f'failed to stop DAS: {err}')


if __name__ == "__main__":

    error_socket_path = '/tmp/das_errors'

    signal.signal(signal.SIGINT, exit_driver)

    if len(sys.argv) > 1:
        if sys.argv[2] == 'send':
            try:
                os.unlink(error_socket_path)
            except OSError:
                if os.path.exists(error_socket_path):
                    raise

    while True:
        try:
            sock_msg.bind((DAS_host, msg_port))
            sock_data.bind((DAS_host, data_port))
            break
        except Exception as err:
            # print('DAS hardware not connected:', err)
            sys.exit(err)
            # time.sleep(3)

    sock_msg.settimeout(0.2)
    sock_data.settimeout(3)

    nics = psutil.net_if_addrs()
    try:
        netInterfaceName = [i for i in nics for j in nics[i] if j.address == DAS_host and j.family == socket.AF_INET][0]
    except Exception:
        netInterfaceName = 'eth0'

    with open('das_driver_config.json') as json_file:
        config_dict = json.load(json_file)

    data_01[8:10] = config_dict['capture_length'].to_bytes(2, 'big')
    data_01[10:12] = config_dict['capture_start'].to_bytes(2, 'big')

    data_02[4:8] = config_dict['frame_period'].to_bytes(4, 'big')
    data_02[8:9] = config_dict['impulse_width'].to_bytes(1, 'big')
    data_02[10:12] = config_dict['impulse_offset'].to_bytes(2, 'big')

    data_03[4:5] = config_dict["diode_current_1"].to_bytes(1, 'big')
    data_03[5:6] = config_dict["diode_peltier_current_1"].to_bytes(1, 'big')
    data_03[6:7] = config_dict["diode_current_2"].to_bytes(1, 'big')
    data_03[7:8] = config_dict["diode_peltier_current_2"].to_bytes(1, 'big')

    data_04[4:6] = config_dict["const_gain_0"].to_bytes(2, 'big')
    data_04[6:7] = config_dict["gain_koef_0"].to_bytes(1, 'big')
    data_04[7:8] = config_dict["control_0"].to_bytes(1, 'big')
    data_04[12:14] = config_dict["const_gain_1"].to_bytes(2, 'big')
    data_04[14:15] = config_dict["gain_koef_1"].to_bytes(1, 'big')
    data_04[15:16] = config_dict["control_1"].to_bytes(1, 'big')

    if len(sys.argv) > 1:
        if sys.argv[1] == 'start':

            while True:
                if start_das(data_01, data_02, data_03, data_04):
                    trace_num = 0
                    data_array = bytearray()
                    data_send, data_pipe = mp.Pipe()
                    data_processor_i = data_processor(data_socket_path='/tmp/das_driver', config_dict=config_dict)
                    data_thread = Thread(target=data_processor_i, args=(data_pipe,), daemon=True).start()
                    in_sync = False
                    time.sleep(1)
                    break
                else:
                    print('DAS hardware FPGA configuration failed. Trying restart hardware...')
                    stop_das()
                    time.sleep(3)

            while True:

                while len(data_array) < config_dict['numTraces'] * config_dict['capture_length'] * 2:
                    try:
                        data_rec = sock_data.recv(10000)
                    except socket.timeout as err:
                        print('DAS ethernet communication breakdown. Reconnecting...', err)
                        data_array = bytearray()
                        result = subprocess.run(['ifconfig', netInterfaceName, 'down'], timeout=2)
                        # print(result.returncode)
                        result = subprocess.run(['ifconfig', netInterfaceName, 'up'], timeout=2)
                        # print(result.returncode)
                        time.sleep(1)
                        print('DAS ethernet restarted')
                        continue

                    # frame_id = int.from_bytes(data_rec[0:3], 'big')
                    subframe_id = int.from_bytes(data_rec[4:5], 'big')
                    if subframe_id == 0:
                        in_sync = True

                    if in_sync:
                        data_array += data_rec[8:]

                if not data_pipe.poll():
                    data_send.send(data_array)
                else:
                    print("\033[91m{}\033[00m".format('ERROR: realtime processing failed'))
                data_array = bytearray()
                in_sync = False

        elif sys.argv[1] == 'stop':
            stop_das()

        elif sys.argv[1] == 'read':
            addr = int(sys.argv[2]).to_bytes(1, 'big')
            data = b'\xcc\xdd\x00' + addr
            sock_msg.sendto(data, (DAS_hardware, msg_port))
            data_rec = sock_msg.recv(64)
            if addr == b'\x01':
                print('control:', int.from_bytes(data_rec[4:8], 'big'))
                print('capture_length:', int.from_bytes(data_rec[8:10], 'big'))
                print('capture_start:', int.from_bytes(data_rec[10:12], 'big'))
            elif addr == b'\x02':
                print('frame_period:', int.from_bytes(data_rec[4:8], 'big'))
                print('impulse_width:', int.from_bytes(data_rec[8:9], 'big'))
                print('impulse_offset:', int.from_bytes(data_rec[10:12], 'big'))
            elif addr == b'\x03':
                print('diode current 1:', int.from_bytes(data_rec[4:5], 'big'))
                print('diode Peltier current 1:', int.from_bytes(data_rec[5:6], 'big'))
                print('diode current 2:', int.from_bytes(data_rec[6:7], 'big'))
                print('diode Peltier current 2:', int.from_bytes(data_rec[7:8], 'big'))
            elif addr == b'\x04':
                print('const_gain_0:', int.from_bytes(data_rec[4:6], 'big'))
                print('gain_koef_0:', int.from_bytes(data_rec[6:7], 'big'))
                print('control_0:', int.from_bytes(data_rec[7:8], 'big'))
                print('reserved:', int.from_bytes(data_rec[8:12], 'big'))
                print('const_gain_1:', int.from_bytes(data_rec[12:14], 'big'))
                print('gain_koef_1:', int.from_bytes(data_rec[14:15], 'big'))
                print('control_1:', int.from_bytes(data_rec[15:16], 'big'))
                print('reserved:', int.from_bytes(data_rec[16:20], 'big'))
        else:
            print('Command unknown')
    else:
        print('No commands')

    sock_msg.close()
    sock_data.close()
