script_folder_demostend  = '../../scripts/' # for demostend
script_folder_local  = 'scripts/' # for laptop
import sys
sys.path.append(script_folder_demostend)
sys.path.append(script_folder_local)
from crop import Сropper
import signal_processing as sp
from creating_datasets import make_tsfresh_structure_online

import numpy as np
import pandas as pd
from tsfresh import extract_features
from joblib import load
import json
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

class classifier_loop(Сropper):
    def __init__(self, model_path, plotting, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = load(model_path)
        self.plotting = plotting
        self.cropper = Сropper(detector_th = self.detector_th,
                              mstd_th =  self.mstd_th,
                              mstd_wdw = self.mstd_wdw,
                              indent = self.indent,
                              N_wait_frames = self.N_wait_frames,
                              max_duration = self.max_duration)
        
        
    def classify(self, signal, plotting = False):
        long_signal = make_tsfresh_structure_online(signal)
        
        data_tsfresh = extract_features(long_signal, column_id='id', column_sort='time',disable_progressbar=True,
                                        kind_to_fc_parameters = sp.get_features_selected(), n_jobs = 3)
        data_tsfresh['sko_32__log_median'] = np.log(data_tsfresh['sko_32__median'])
        data_tsfresh['sko_32__log_length'] = np.log(data_tsfresh['sko_32__length'])
        data_tsfresh['median_128__log_standard_deviation'] = np.log(data_tsfresh['median_128__standard_deviation'])
        data_tsfresh.drop(['median_128__standard_deviation'],axis = 1, inplace=True)
        data_tsfresh.drop(['sko_32__length'],axis = 1, inplace=True)
        data_tsfresh.drop(['sko_32__median'],axis = 1, inplace=True)
        data_tsfresh = data_tsfresh[self.model.feature_names_in_] # sort names of features
       
        
        prob = self.model.predict_proba(data_tsfresh).round(2)
        classes = self.model.classes_
        model_predictions = dict(zip(classes, *prob))
        if self.plotting:
            fig, ax = plt.subplots(1, 2, figsize = (10,6))
            ax[0].plot(signal)
            ax[0].set_xlabel('Time, ms')
            ax[0].set_ylabel('Amplitude')
            ax[0].set_title('Interaction (raw signal from interferometer)')
            ax[0].grid()
            
            norm = mcolors.Normalize(vmin=0, vmax=1)
            cmap = mcolors.LinearSegmentedColormap.from_list("custom_red", ["#ffcccc", "#ff0000"])
            colors = cmap(norm(*prob))
            
            ax[1].bar(classes, *prob, color = colors, alpha = 0.9, edgecolor='black')
            ax[1].set_xlabel('Classes')
            ax[1].set_title('Probability_distribution')
            ax[1].set_ylim([0, 1])
            plt.show()
        return model_predictions
    def classifier_on(self):
        while True:
            buffer_data = sys.stdin.buffer.read(80000)
            current_data = sp.central_chl(np.frombuffer(buffer_data))
            self.stored_signal = self.cropper(current_data)
            if self.stored_signal is None:
                pass
            else:
                print(json.dumps(self.classify(self.stored_signal)))
                sys.stdout.flush()
