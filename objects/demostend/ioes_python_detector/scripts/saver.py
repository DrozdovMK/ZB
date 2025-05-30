from datetime import datetime
import numpy as np
import h5py
import os
import json

class Saver():
    def __init__(self, save_path, zone_num, max_files_count):
        self.save_path = save_path
        self.zone_num = zone_num
        self.max_files_count = max_files_count
    def save_alarm(self, signal, probabilities):
        now = datetime.now()
        date = now.strftime("%Y_%m_%d")
        hour = f"{now.hour:02d}" + "h"
        self.filepath = os.path.join(self.save_path,
                                     str(self.zone_num),
                                     date,
                                     hour)
        if not os.path.exists(self.filepath):
            os.makedirs(self.filepath)
            marker_filepath = os.path.join(self.save_path, "marker.txt")
            with open(marker_filepath, "w") as file:
                file.write("This directory contains datasets")
        filename = "{}_{}.{}".format(self.zone_num,
                                     now.strftime("%Y_%m_%d_%H_00_00_000"), "hdf5")
        
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
                    
            dataset = file.create_dataset(f"alarm {current_index:05d}", data=np.array(signal))
            dataset.attrs["probabilities"] = json.dumps(probabilities)
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            dataset.attrs["date_time"] = dt_string