import sys
import numpy as np
import json
import asyncio
import multiprocessing as mp
from threading import Timer
import matplotlib.pyplot as plt
from detector import Detector
from cropper import Cropper
from preprocessing import Preprocessor, central_chl
from classifier import Classifier
from saver import Saver
from data_client import connect_to_data_server
script_folder  = 'scripts/'
sys.path.append(script_folder)

class Mainloop():
    """
    model_path - путь откуда берется модель
    data_socket_path - имя unix сокета через который получаем данные с драйвера
    indent_time - отступ от начала тревоги (кол-во отсчетов)
    cooling_time - время охлаждения
    max_time - максимальное время сигнала
    threshold - порог детектора (во сколько раз превышение СКО)
    plotting - строить ли график тревоги?
    verbose - выводить ли в консоль тревогу?
    save_path - путь для сохранения тревог
    zone_num - номер зоны на объекте
    max_files_count - максимальное кол-во файлов для сохранения
    saving - сохранять ли тревоги по директории save_path?
    """
    def __init__(self,
                 model_path: str,
                 data_socket_path: str,
                 indent_time: int,
                 cooling_time: int,
                 max_time: int,
                 threshold: float,
                 plotting: bool,
                 verbose: bool,
                 save_path: str,
                 zone_num: int,
                 max_files_count: int,
                 saving: bool):
        self.zone_num = zone_num
        self.detector = Detector(threshold)
        self.cropper = Cropper(indent_time, cooling_time, max_time, detector=self.detector)
        self.preprocessor = Preprocessor()
        self.classifier = Classifier(model_path=model_path, preprocessor=self.preprocessor)

        self.verbose = verbose # bool: Выводить ли в консоль предсказания?
        self.plotting = plotting # bool: Строить ли графики предсказаний (для отладки)?
        self.saving = saving # bool: Сохранять ли тревоги в snapshots?
        self.driver_one_second = 1024 # настройка для драйвера
        self.driver_delimiter = bytearray(b'<<<EndOfData.>>>') # разделитель конца сообщения
        self.data_socket_path = data_socket_path
        
        with open('das_config.json') as json_file:
            self.config_dict = json.load(json_file)
        if self.saving:
            assert save_path, "You should specify save path"
            assert zone_num, "You should specify zone number"
            self.saver = Saver(save_path, zone_num, max_files_count)
                
    async def start(self):

        reader, _ = await connect_to_data_server(self.data_socket_path)
        buffer = bytearray()
        data_window = np.array([])
        while True:
            try:
                data = await reader.readuntil(separator=self.driver_delimiter)
                buffer += data
                delimiterJSON = buffer.index(b'\0')
                jsonHdrBytes = buffer[:delimiterJSON]
                jsonHdrObj = json.loads(jsonHdrBytes.decode())
                numTraces = jsonHdrObj['numTraces']
                traceSize = jsonHdrObj['traceSize']
                byteData = buffer[delimiterJSON + 1: delimiterJSON + numTraces * traceSize * 2 + 1]
                data_np = np.frombuffer(byteData, dtype=np.uint16).reshape(numTraces, traceSize)
                data_window = np.vstack((data_window, data_np)) if data_window.size else data_np
                
                if data_window.shape[0] >= self.config_dict['time_window'] + self.driver_one_second:
                    data_window = np.roll(data_window, shift=-self.driver_one_second, axis=0)
                    data_window = np.delete(data_window, np.s_[-self.driver_one_second:], axis=0)
                    print(data_window.shape)
                    self.stored_signal = self.cropper(data_window[:, self.zone_num])
                    if self.stored_signal is None:
                        pass
                    else:
                        print('signal collected', self.stored_signal.shape)
                        predictions = self.classifier.predict(self.stored_signal)
                        if self.plotting:
                            self.classifier.plot(self.stored_signal, predictions)
                        if self.saving:
                            self.saver.save_alarm(self.stored_signal, predictions)
                        if self.verbose:
                            # вывожу в консоль
                            print(json.dumps(predictions))
                            sys.stdout.flush()
                buffer.clear()
            except asyncio.exceptions.LimitOverrunError as e:
                buffer += await reader.read(e.consumed)
            except asyncio.exceptions.IncompleteReadError:
                buffer.clear()
                reader, _ = await connect_to_data_server(self.data_socket_path)

    def start_test(self, path_to_test_signal, step=1000):
        test_data = np.load(path_to_test_signal)
        curr_idx = 0
        while curr_idx < len(test_data):
            current_data = test_data[curr_idx: np.clip(curr_idx + step, 0, len(test_data)-1)]
            self.stored_signal = self.cropper(current_data)
            if self.stored_signal is None:
                print("=============")
                print(f"Индексы = [{curr_idx}, {curr_idx + step}]")
                print(f"Превышение СКО в {np.std(current_data) / self.detector.noise_std}")
                print(f"Тревоги нет, флажок тревоги {self.cropper.alarm_flag}")
                print(f"Кэш {len(self.cropper.cached_frames)}")
                pass
            else:
                print("=============")
                print(f"Индексы = [{curr_idx}, {curr_idx + step}]")
                predictions = self.classifier.predict(self.stored_signal)
                if self.verbose:
                    print(json.dumps(predictions))
                    sys.stdout.flush()
                if self.plotting:
                    self.classifier.plot(self.stored_signal, predictions)
                if self.saving:
                    self.saver.save_alarm(self.stored_signal, predictions)
            curr_idx += step
        
if __name__ == "__main__":
    mainloop = Mainloop(model_path="...",
                        indent_time=500,
                        cooling_time=1000,
                        max_time=10000,
                        threshold=2,
                        plotting=False,
                        verbose=True,
                        saving=True,
                        save_path="...",
                        zone_num=...,
                        max_files_count=250,
                        data_socket_path = '/tmp/das_driver')
    mainloop.start()
