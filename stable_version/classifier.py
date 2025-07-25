import sys
import json
import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
# здесь важно импортировать из preprocessing все (*)!
# Хотя в коде нет явного вызова модулей из preprocessing,
# классификтор состоит из блоков, описанных в preprocessing
from preprocessing import *

class Classifier():
    """
    Класс классификатора для применения в режиме реального времени
    """
    def __init__(self, model_path):
        """
        Args:
            model_path (str): путь до обученной модели
            preprocessor (Preprocessor): объект предобработчика, который
            преобразует np.ndarray -> longDataFrame.
            
        """
        self.model = joblib.load(model_path)
        self.classes = self.model["classifier"].classes_

    def predict(self, signal: np.ndarray) -> dict[str,float]:
        """
        Метод для предсказания метки класса по полученному сигналу
        Этот метод должен подавать на вход классификатора данные в формате long_df
        А возвращать в формате, который требуется дальше (Кирилл просит json с вероятностями по классам)
        Поэтому я возвращаю dict
        
        signal (np.ndarray) - обрезанный после cropper сигнал
        Возвращает:
        dict: ключи - имена классов, значения - вероятности
        соответствующих классов.
        """
        # Преобразование в формат tsfresh
        long_df = pd.DataFrame({
            'id': [0]*len(signal),
            'time': range(len(signal)),
            'signal_raw': signal,
        })
        
        prob = self.model.predict_proba(long_df).round(2)
        model_predictions = dict(zip(self.classes, *prob))
        return model_predictions
    
    def plot(self, signal, model_predictions):
        """
        Метод для отладки, который нужен чтобы строить графики сырого сигнала
        и соответствующего распределения классификатора по классам
        """
        fig, ax = plt.subplots(1, 2, figsize = (10,6))
        ax[0].plot(signal)
        ax[0].set_xlabel('Time, ms')
        ax[0].set_ylabel('Amplitude')
        ax[0].set_title('Interaction (raw signal from interferometer)')
        ax[0].grid()
        ax[0].set_ylim([0, 16000])
        
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
