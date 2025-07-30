import sys
import numpy as np
import json
from detector import Detector
from cropper import Cropper
from classifier import Classifier
from saver import Saver
import multiprocessing as mp
script_folder  = 'scripts/'
sys.path.append(script_folder)
import time

class Mainloop():
    """
    Класс который объединяет все составляющие: 
    * Detector (определяет наличие тревоги)
    * Cropper (реализует обрезку сырого сигнала в соответствии с
    необходимым отступом и временем охлаждения)
    * Preprocessor (обрезанный сырой сигнал преобразуется в 
    longFreqDataFrame и longDataFrame)
    * Classifier (модель выдает классификацию)
    * Saver (сигнал и метка классификации сохраняются в 
    логи системы для дальнейшего просмотра, разметки при помощи GUI_viewer)
    """
    def __init__(self,
                 model_path,
                 qOut,
                 indent_time = 500,
                 cooling_time=1000,
                 max_time=10000,
                 threshold=2,
                 plotting=False,
                 verbose=True,
                 save_path=None,
                 zone_num=None,
                 max_files_count=250,
                 saving=False):
        """
        Задает все необходимые параметры для Detector, Cropper, Preprocessor, Classifier

        Args:
            model_path (str): Путь к файлу с обученной моделью
            
            qOut (mp.Queue): Выходная очередь в которую передается сигнал о тревоге от
            классификатора
            
            indent_time (int, optional): Время отступа перед началом тревоги.
            (измеряется в количестве отсчетов). По умолчанию = 500.
            
            cooling_time (int, optional): Время охлаждения системы (в кол-ве отсчетов)
            По умолчанию = 1000.
            
            max_time (int, optional): Максимальное время тревоги (в кол-ве отсчетов).
            По умолчанию = 10000.
            
            threshold (int, optional): Порог срабатывания детектора (превышение СКО).
            По умолчанию = 2.
            
            plotting (bool, optional): Строить ли графики?  По умолчанию False.
            
            verbose (bool, optional): Выводить ли в консоль классификацию? По умолчанию True.
            
            save_path (str, optional): Путь для сохранения тревог (наборов датасетов).
            По умолчанию None
            
            zone_num (int, optional): Номер зоны охранной системы .По умолчанию None.
            
            max_files_count (int, optional): Максимальное кол-во тревог в одном файле.
            По умолчанию 250.
            
            saving (bool, optional): Сохранять ли тревоги в save_path? По умолчанию False.
        """
        self.detector = Detector(threshold)
        self.cropper = Cropper(indent_time, cooling_time, max_time, detector=self.detector)
        self.classifier = Classifier(model_path=model_path)
        self.qOut   = qOut
        self.verbose = verbose
        self.plotting = plotting
        self.saving = saving
        self.zone_num = zone_num
        if self.saving:
            assert save_path, "You should specify save path"
            assert zone_num, "You should specify zone number"
            self.saver = Saver(save_path, zone_num, max_files_count)
    def receive(self, data):
        """
        Метод для классификации/набора полученного сигнала.
        
        data - сигнал по одной зоне
        """
        self.stored_signal = self.cropper(data)
        if self.stored_signal is not None:
            predictions = self.classifier.predict(self.stored_signal)
            alarm_name, alarm_prob = max(predictions.items(), key=lambda x: x[1])
            if self.verbose:
                self.qOut.put({'nchn':self.zone_num, 'width': 1, 'alarm':alarm_name, 'timestamp':time.time()})
            if self.saving:
                self.saver.save_alarm(self.stored_signal, predictions)
            if self.plotting:
                self.classifier.plot(self.stored_signal, predictions)
    def start_test(self, path_to_test_signal:str, step:int=1000):
        """
        Тестовая программа для проверки работы системы.

        Args:
            path_to_test_signal (str): Путь до файла .npy в котором в 
            непрерывном режмие записан сигнал с интерферометра
            (например 60 секунд с различными воздействиями)
            
            step (int, optional): Размер порции данных, поступающих за раз.
            По умолчанию 1000.
        """
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
    # Пример запуска
    
    # Номер зоны задается со стороны бэкенда Кирилла, мне он приходит как
    # параметр командной строки (sys.argv[1])
    zone_num = ...
    mainloop = Mainloop(model_path="pipeline_with_kashira.pkl",
                        indent_time=500,
                        cooling_time=1000,
                        max_time=10000,
                        threshold=2,
                        plotting=False,
                        verbose=True,
                        saving=True,
                        save_path="./data",
                        zone_num=...,
                        max_files_count=250)
    mainloop.start()
