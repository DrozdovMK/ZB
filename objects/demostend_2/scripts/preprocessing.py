import os
import h5py
import pandas as pd
import numpy as np
from tsfresh.feature_extraction.settings import EfficientFCParameters

class TsfreshDatasetTransformer():
    """
    Класс предназначен для приведения данных в формат хранения временных рядов,
    который используется в tsfresh (далее этот формат называется longDataFrame)
    Ссылка на документацию: https://tsfresh.readthedocs.io/en/latest/text/data_formats.html
    
    Класс имеет методы:
    * online_transform - метод для перевода массива numpy в longDataFrame.
    (Используется при работе в режиме реального времени на объекте)
    
    * make_tsfresh_structure_from_simple_directory - метод для парсинга сохраненных данных
    из директории в которой хранятся hdf5 файлы в формате:
    один hdf5 файл - одно воздействие. 
    Возвращает кортеж (longDataFrame, labels)
    
    * make_tsfresh_structure_from_nested_directory - метод для парсинга сохраненных данных
    из директории в которой файлы хранятся во вложенном формате 
    (зона -> день -> час -> hdf5 файл)
    Возаращает кортеж (longDataFrame, labels)
    """
    def __init__(self):
        pass
    def online_transform(self, signal: np.ndarray) -> pd.DataFrame:
        df = pd.DataFrame({
            'id': [0]*len(signal),
            'time': range(len(signal)),
            'signal_raw': signal,
        })
        return df

    def make_tsfresh_structure_from_simple_directory(self, directory_path: str):
        '''
        {Историческая справка:
        Раньше данные каждого типа воздействия писались в соответствующий hdf5 файлы.
        Потом структура сохранения файлов поменялась (принято решение о использовании
        более надежной и отказоустойчивой системе хранения). Однако данные записанные
        когда-то давно остались, их не хотелось выкидывать. Поэтому был создан этот метод}
        
        * directory_path - директория в которой хранятся hdf5 файлы.
        Названия файлов означают метку класса, которой соответствуют 
        сигналы внутри.
        
        Например в directory_path могут быть файлы:
        hit.hdf5 (68 воздействий)
        saw.hdf5 (75 воздействий)
        perelaz.hdf5 (54 воздействия)
        
        На выходе будет (data, label) 
        * data pd.DataFrame в соответствии с форматом для дальнейшего extract_features
        библиотеки tsfresh (longDataFrame)
        * label в формате pd.Series 
        (например pd.Series([hit, hit, ..., saw, perelaz, hit, ...]))
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

    def make_tsfresh_structure_from_nested_directory(self, directory_path):
        """
        {Историческая справка:
        Структура хранения файлов во вложенном (nested) формате (см. пример ниже)
        используется для того, чтобы разделить тревоги по часам, чтобы в случае отказа
        системы или повреждения файла были потеряны тревоги за один час а не все
        тревоги записанные ранее} 
        
        Актуальная версия для трансформатора датасета
        На вход: директория со вложенными папками, например:
        - Общее название (название объекта)
            - 566 (номер зоны)
                - 2024_10_29 (дата)
                    - 01h (Час тревоги)
                        566_2024_10_29_01_00_00_000.hdf5 (название файла с тревогами)
                    - 02h
                        566_2024_10_29_02_00_00_000.hdf5
                    - 12h
                        ...
                    - 18h
                        ...
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
    """
    Класс для создания новых колонок в longDataFrame: 
    1) скользящее среднее signal_raw (signal_mean256)
    2) скользящее СКО signal_raw (signal_std32)
    Создание новых колонок в longDataFrame нужно для того чтобы создать
    отфильтрованые версии сырого сигнала, которые обладают более устойчивыми 
    статистическими характеристиками. 
    
    signal_mean256 - дает более подробную информацию об постоянной составляющей сигнала
    signal_std32 - убирает постоянную составляющую, несет больше информации о переменной 
    составляющей сигнала.
    
    Размеры окон (256 и 32) подобраны эмпирически. 
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
        longDataFrame = X.copy()
        for name, transformation in self.transformations:
            longDataFrame[name] = (
                longDataFrame.
                groupby("id")["signal_raw"].
                transform(transformation)
            )
        return longDataFrame

class FourierColumnCreator():
    """
    Для анализа сигнала в частотной области, longDataFrame 
    (до добавления отфильтрованных колонок из прошлого метода)
    преобразуется в фурье-спектр, сгруппированный по частотам.
    (Основной интерес представляют более низкие частоты, 
    поэтому по фурье-спектру проходится скользящий фильтр среднего
    с переменным окном, размер окна увеличивается экспоненциально)
    
    Возвращает новый longFreqDataFrame в котором хранятся обработанные
    частотные спектры.

    """
    def __init__(self, augmenter=None):
        self.augmenter = augmenter
        
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
        longFreqDataFrame = pd.DataFrame()
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
            longFreqDataFrame = pd.concat([longFreqDataFrame, temp_df])
        return longFreqDataFrame.reset_index(drop=True)
    
class FeaturesToTsfresh():
        """
        Класс, предназначенный для получения списка статистических признаков
        (см. https://tsfresh.readthedocs.io/en/latest/text/feature_extraction_settings.html)
        
        методы:
        * get_time_domain_features - получение dict с фичами для longDataFrame
        * get_freq_domain_features - получение dict с фичами для longFreqDataFrame
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
    """
    Класс, соединяющий процесс от получения данных до создания новых колонок
    1) Получение данных (в режиме реального времени или из директории)
    в формате longDataFrame
    2) Преобразование в longDataFrame с новыми колонками (с mean256 и std32)
    Также получение longFreqDataFrame с Фурье-спектрами.
    """
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
        return spectrum_signal, filter_signal

def central_chl(data, len_const=10):
    return data[5:10000:len_const]
