import os
import h5py
import pandas as pd
import numpy as np
from functools import cache
from tsfresh.feature_extraction.settings import EfficientFCParameters

class TsfreshDatasetTransformer():
    def __init__(self):
        pass
    def online_transform(self, signal):
        df = pd.DataFrame({
            'id': [0]*len(signal),
            'time': range(len(signal)),
            'signal_raw': signal,
        })
        return df
    @cache
    def make_tsfresh_structure_from_simple_directory(self, directory_path):
        '''
        Устаревшая функция (Для работы с данными, набранными ранее)
        * directory_path - директория в которой хранятся hdf5 файлы. Названия файлов означают метку класса, которой соответствуют 
        сигналы внутри.
        
        Например в directory_path могут быть файлы:
        hit.hdf5 (68 воздействий)
        saw.hdf5 (75 воздействий)
        perelaz.hdf5 (54 воздействия)
        
        * sko_window, mean_window, median_window - окна для скользящего ско, среднего и медианы соответственно.
        
        На выходе будет (data, label) 
        * data pd.DataFrame в соответствии с форматом для дальнейшего extract_features библиотеки tsfresh
        * label в формате pd.Series (например pd.Series([hit, hit, ..., saw, perelaz, hit, ...]))
        '''
        long_df = pd.DataFrame()
        labels = []
        i = 0 # счетчик для колонки id
        # Проходим по всем файлам в директории
        for filename in os.listdir(directory_path):
            if filename.endswith(".h5") or filename.endswith(".hdf5"):
                # Полный путь к файлу
                filepath = os.path.join(directory_path, filename)
                # Название класса это имя файла без расширения
                class_label = os.path.splitext(filename)[0]  
                with h5py.File(filepath, 'r') as hdf_file:
                    for key in hdf_file.keys():
                        sequence = hdf_file[key][()]
                        # Создаем DataFrame для текущего временного ряда
                        temp_df = pd.DataFrame({
                            'id': [int(i)] * len(sequence),
                            'time': range(len(sequence)),
                            'signal_raw': sequence,
                        })
                        
                        long_df = pd.concat([long_df, temp_df])
                        labels.extend([class_label])
                        i+=1
        return long_df.reset_index(drop=True), pd.Series(labels)
    @cache
    def make_tsfresh_structure_from_nested_directory(self, directory_path):
        """
        Актуальная версия для трансформатора датасета
        На вход: директория со вложенными папками, например:
        - Общее название (название объекта)
            - 566 (номер зоны)
                - 2024_10_29 (дата)
                    - 01h (Час тревоги)
                        566_2024_10_29_01_00_00_000.hdf5 (название файла с тревогами)
                    - 02h
                    - 12h
                    - 18h
                - 2024_10_30
                    - ...
                - 2024_10_31
                    - ...
            - 454
                - ...
            - 321
                - ...
        Внутри файлов .hdf5 находятся тревоги с названием alarm 00001, alarm 00002, ...
        alarm {i}  является датасетом с аттрибутами: истинная метка класса, порог классификации и т.д.
        
        На выходе ожидается long_dataframe в соответствие с требованиями от sklearn
        """
        long_df = pd.DataFrame() # датафрейм для данных
        labels = []
        i = 0 # счетчик для колонки id
        zones_list = next(os.walk(directory_path))[1] # список зон (только папки)
        for zone in zones_list:
            zone_path = os.path.join(directory_path, zone)
            dates_list = next(os.walk(zone_path))[1] # список дат (только папки)
            for date in dates_list:
                date_path = os.path.join(zone_path, date)
                hours_list = next(os.walk(date_path))[1] # список часов (только папки)
                for hour in hours_list:
                    hour_path = os.path.join(date_path, hour)
                    filename = os.listdir(hour_path)[0]
                    filename = os.path.join(hour_path, filename)
                    with h5py.File(filename, "r") as hdf_file:
                        for key in hdf_file.keys():
                            sequence = hdf_file[key][()]
                            temp_df = pd.DataFrame({
                                'id': [int(i)] * len(sequence),
                                'time': range(len(sequence)),
                                'signal_raw': sequence
                            })
                            if "label" not in hdf_file[key].attrs.keys():
                                labels.append("unknown")
                                print("Found unknown label in zone {} \ndate: {}".format(
                                    zone,
                                    hdf_file[key].attrs["date_time"]
                                ))
                            else:
                                labels.append(hdf_file[key].attrs["label"])
                            long_df = pd.concat([long_df, temp_df])
                            i += 1
        return long_df.reset_index(drop=True), pd.Series(labels)

class FilterColumnCreator():
    """Делает колонки отфильтрованного сигнала
    (transform возвращает датафрейм с доп колонками отфильтрованного сигнала)
    """
    def __init__(self):
        self.transformations = [("signal_std32", lambda x: 
                                    (self.min_max(x).rolling(window=32, min_periods=1, center=True).std())),
                                ("signal_mean256", lambda x: 
                                    (self.min_max(x).rolling(window=256, min_periods=1, center=True).mean()))]
    
    def min_max(self, signal, columns_for_df=None):
        if isinstance(signal, pd.DataFrame):
            if columns_for_df ==  None:
                columns_for_df = signal.columns
            return (signal[columns_for_df] - np.min(signal[columns_for_df], axis = 0))/(
                np.max(signal[columns_for_df], axis=0) - np.min(signal[columns_for_df], axis = 0))
        else:
            return (signal - np.min(signal))/(np.max(signal) - np.min(signal))
    
    def transform(self, X:pd.DataFrame):
        if self.transformations==None:
            return X
        X_transformed = X.copy()
        for name, transformation in self.transformations:
            X_transformed[name] = (
                X_transformed.
                groupby("id")["signal_raw"].
                transform(transformation)
            )
        return X_transformed

class FourierColumnCreator():
    """Делает колонки спектра Фурье сгруппированного по частотам
    (transform возвращает датафрейм с колонкой отфильтрованного сигнала)
    """
    def __init__(self, augmenter=None):
        self.augmenter = augmenter
        pass
    def binned_fourier_transform(self, signal, n_bins=70, fs=1000):
        N = len(signal)
        Ts = 1/fs
        fftfreqz = np.fft.fftfreq(N, Ts)[:N//2]
        signal_fft = 2/N * np.abs(np.fft.fft(signal))[:N//2]
        bin_edges = np.logspace(start=np.log2(fftfreqz[1]),
                                stop=np.log2(fftfreqz[-1]),
                                num=n_bins+1,
                                base=2)
        binned_fft = []
        binned_freqz = []
        for i in range(n_bins):
            bin_mask = (fftfreqz >= bin_edges[i]) & (fftfreqz < bin_edges[i + 1])
            if np.any(bin_mask):
                binned_fft.append(np.mean(signal_fft[bin_mask]))
                binned_freqz.append(np.mean(fftfreqz[bin_mask]))
        return np.array(binned_fft)
    
    def transform(self, X:pd.DataFrame):
        result_df = pd.DataFrame()
        indexes = X["id"].unique()
        result = (
            X.groupby("id")
            ["signal_raw"].
            apply(func=lambda x : self.binned_fourier_transform(
                signal=x,
                n_bins=100,
                fs=1000,
                ))
            )
        for idx in indexes:
            temp_data = result[idx]
            temp_df = pd.DataFrame({
                'id': [int(idx)] * len(temp_data),
                'freq_num': range(len(temp_data)),
                'signal_binned_fft': temp_data
            })
            result_df = pd.concat([result_df, temp_df])
        return result_df.reset_index(drop=True)
    
class FeaturesToTsfresh():
        """Здесь указано какие фичи забирать из фильтрованных колонок и колонки со спектром
        """
        def __init__(self, ):
            pass
        def get_time_domain_features(self):
            sett_answ = {
                "signal_raw" : 
                    {
                        "skewness":None,
                        "standard_deviation":None,
                        "length": None,
                        "fft_aggregated": [{"aggtype": "centroid"},
                                        {"aggtype": "variance"},
                                        {"aggtype": "skew"},
                                        {"aggtype": "kurtosis"}]
                    },
                    
                "signal_std32" :
                    {
                    "standard_deviation": None,
                    },
                    
                "signal_mean256": 
                    {
                        "standard_deviation": None,
                        "kurtosis": None,
                    }
                }
            return sett_answ
        def get_freq_domain_features(self):
            sett_answ = EfficientFCParameters()
            sett_answ.pop("fft_coefficient", None)
            sett_answ.pop("cwt_coefficients", None)
            sett_answ.pop("fft_aggregated", None)
            sett_answ.pop("number_cwt_peaks", None)
            sett_answ.pop("spkt_welch_density", None)
            sett_answ.pop("fourier_entropy", None)
            sett_answ.pop("query_similarity_count", None)
            sett_answ.pop("symmetry_looking", None)
            sett_answ.pop("large_standard_deviation", None)
            
            return sett_answ

class Preprocessor():
    def __init__(self):
        self.long_transformer = TsfreshDatasetTransformer()
        self.freq_transformer = FourierColumnCreator()
        self.filter_transformer = FilterColumnCreator()
        
    def transform(self, signal, from_numpy = True):
        if from_numpy:
            long_signal = self.long_transformer.online_transform(signal)
        else:
            long_signal = signal
        spectrum_signal = self.freq_transformer.transform(long_signal)
        filter_signal = self.filter_transformer.transform(long_signal)
        return long_signal, spectrum_signal, filter_signal

def central_chl(data, len_const=10):
    return data[5:10000:len_const]
