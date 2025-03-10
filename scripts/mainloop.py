import sys
import numpy as np
import json
from detector import Detector
from cropper import Cropper
from preprocessing import Preprocessor, central_chl
from classifier import Classifier
from saver import Saver

script_folder  = 'scripts/'
sys.path.append(script_folder)

class Mainloop():
    def __init__(self, model_path, indent_time = 500, cooling_time=1000, max_time=10000,
                 threshold=2, plotting=False, verbose=True,
                 save_path=None, zone_num=None, max_files_count=250, saving=False):
        
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
    def start(self):
        while True:
            buffer_data = sys.stdin.buffer.read(80000)
            current_data = central_chl(np.frombuffer(buffer_data))
            self.stored_signal = self.cropper(current_data)
            if self.stored_signal is None:
                pass
            else:
                predictions = self.classifier.predict(self.stored_signal)
                if self.verbose:
                    print(json.dumps(predictions))
                    sys.stdout.flush()
                if self.saving:
                    self.saver.save_alarm(self.stored_signal, predictions)
                if self.plotting:
                    self.classifier.plot(self.stored_signal, predictions)
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
                        max_files_count=250)
    mainloop.start()