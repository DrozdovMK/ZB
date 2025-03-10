script_folder_demostend  = '../../scripts/' # for demostend
script_folder_local  = 'scripts/' # for laptop
import sys
import os
sys.path.append(script_folder_demostend)
sys.path.append(script_folder_local)
from scripts.cropper import Сropper
import h5py
import signal_processing as sp
import numpy as np
from datetime import datetime
from datetime import timedelta
import json
from classifier import classifier_loop

class HDF5Handler(classifier_loop):
    
    def __init__(self, object_name, zone, max_files_count, verbose, saving, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_name = object_name
        self.zone = zone
        self.max_files_count = max_files_count
        self.cropper = Сropper(detector_th = self.detector_th,
                              mstd_th =  self.mstd_th,
                              mstd_wdw = self.mstd_wdw,
                              indent = self.indent,
                              N_wait_frames = self.N_wait_frames,
                              max_duration = self.max_duration)
        
        self.verbose = verbose # bool: выводить ли в консоль предсказания
        self.saving = saving # bool: сохранять ли логи

    def add_signal(self, data, probabilities):
        """Добавляет сигнал в HDF5 файл."""
        now = datetime.now()
        date = now.strftime("%Y_%m_%d")
        hour = f"{now.hour:02d}" + "h"
        self.filepath = os.path.join(self.object_name, 
                                     str(self.zone),
                                     date,
                                     hour)
        if not os.path.exists(self.filepath):
            os.makedirs(self.filepath)
            marker_filepath = os.path.join(self.object_name, "marker.txt")
            with open(marker_filepath, "w") as file:
                file.write("This directory contains datasets")
        filename = "{}_{}.{}".format(self.zone,
                                  now.strftime("%Y_%m_%d_%H_00_00_000"),
                                  "hdf5")
        self.filename = os.path.join(self.filepath, filename)
        with h5py.File(self.filename, 'a') as file:
            current_len = len(file)
            # проверяем количество датасетов в hdf5
            if current_len == 0:
                current_index = 1
            elif current_len < self.max_files_count:
                current_index = int(list(file.keys())[-1].split()[-1]) + 1
            elif current_len >= self.max_files_count:
                del file[list(file.keys())[0]]
                for key in list(file.keys()):
                    i = key.split()[-1]
                    file[f"alarm {int(i)-1:05d}"] = file[f"alarm {int(i):05d}"]
                    del file[f"alarm {int(i):05d}"]
                current_index = int(list(file.keys())[-1].split()[-1]) + 1
                    
            dataset = file.create_dataset(f"alarm {current_index:05d}", data=np.array(data))
            dataset.attrs["probabilities"] = json.dumps(probabilities)
            dataset.attrs["detector_threshold"] = self.detector_th
            dataset.attrs["sko_window"] = self.mstd_wdw
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            dataset.attrs["date_time"] = dt_string
    
    def saver_on(self,):
        while True:
            buffer_data = sys.stdin.buffer.read(80000)
            current_data = sp.central_chl(np.frombuffer(buffer_data))
            self.stored_signal = self.cropper(current_data)
            if self.stored_signal is None:
                pass
            else:
                model_preds_str = json.dumps(self.classify(self.stored_signal))
                if self.saving:
                    self.add_signal(data = self.stored_signal,
                                    probabilities= model_preds_str)
                if self.verbose:
                    print(model_preds_str)
            sys.stdout.flush()
if __name__ == "__main__":
    # Пример использования
    hdf5_handler = HDF5Handler(
        object_name='demostend',
        model_path = 'RnD/classifier_v2_rf/random_forest.pkl',
        zone = ...,
        detector_th = 150,
        mstd_th = 150,
        mstd_wdw = 32,
        N_wait_frames = 2,
        indent = 500,
        max_duration = 25000,
        max_files_count=10,
        saving= True,
        verbose = True      
    )

