import sys
import json
from joblib import load
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

from preprocessing import (Preprocessor, central_chl)

class Classifier():
    def __init__(self, model_path, preprocessor):
        
        self.model = load(model_path)
        self.classes = self.model["model"].classes_
        self.preprocessor = preprocessor

    def predict(self, signal):
        # Преобразование в формат tsfresh
        long_signal, spectrum_signal, filter_signal = self.preprocessor.transform(signal)
        
        # Установка контейнера с временным рядом для работы pipeline (требование tsfresh)
        self.model["feature_extraction"]["fft_features_extractor"].set_params(
            augmenter__timeseries_container = spectrum_signal
        )
        self.model["feature_extraction"]["time_features_extractor"].set_params(
            augmenter__timeseries_container = filter_signal
        )
        X_idx = pd.DataFrame(index=[0])
        
        prob = self.model.predict_proba(X_idx).round(2)
        model_predictions = dict(zip(self.classes, *prob))
        return model_predictions
    
    def plot(self, signal, model_predictions):
        fig, ax = plt.subplots(1, 2, figsize = (10,6))
        ax[0].plot(signal)
        ax[0].set_xlabel('Time, ms')
        ax[0].set_ylabel('Amplitude')
        ax[0].set_title('Interaction (raw signal from interferometer)')
        ax[0].grid()
        ax[0].set_ylim([0, 16500])
        norm = mcolors.Normalize(vmin=0, vmax=1)
        cmap = mcolors.LinearSegmentedColormap.from_list("custom_red", ["#ffcccc", "#ff0000"])
        
        prob = list(model_predictions.values())
        classes = list(model_predictions.keys())
        colors = cmap(norm(prob))
        
        ax[1].bar(classes, prob, alpha = 0.9, edgecolor='black')
        ax[1].set_xlabel('Classes')
        ax[1].set_title('Probability_distribution')
        ax[1].set_ylim([0, 1])
        plt.show()

        
