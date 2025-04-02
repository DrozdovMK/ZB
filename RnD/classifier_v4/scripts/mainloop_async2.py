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
        
        
        if self.saving:
            assert save_path, "You should specify save path"
            assert zone_num, "You should specify zone number"
            self.saver = Saver(save_path, zone_num, max_files_count)
                
    async def start(self):

        data_window = np.array([])
        while True:
            data = sys.stdin.read(1024)
            buffer += data
            
            self.stored_signal = self.cropper(data)
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
    
    if len(sys.argv) < 2:
        raise Exception(f"You should specify zone_config, sys.argv:{sys.argv}")
    zone_config = sys.argv[1]
    
    mainloop = Mainloop(**zone_config)
    mainloop.start()
