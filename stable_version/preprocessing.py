import os
import h5py
import pandas as pd
import numpy as np
from tsfresh.transformers import FeatureAugmenter
from sklearn.base import BaseEstimator, TransformerMixin

class TsfreshDatasetTransformer():
    """
    Класс предназначен для парсинга данных из директории в формат хранения временных рядов,
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


class ParseToPandas():
    """
    (использует в своей основе TsfreshDatasetTransformer из preprocessing)
    
    Класс предназначени для соединения данных, которые были взяты из разных источников:
    1) Папок со вложенной структурой (список путей в paths_to_nested)
    2) Папок без вложенной структуры (один hdf5 - один тип воздействий) (список путей в old_paths)
    
    Парсинг происходит при вызове инициализатора. При вызове __init__ возвращается 
    pd.DataFrame с тремя колонками: id, time, signal_raw
    
    Есть вспомогательные методы ():
    * concat_datasets - объединяет данные с нескольких источников
    
    * rename_idxs - переименовывает id у воздействий после склейки
    
    * drop_classes - удаляет некоторые типы воздействий из датасета.
    (Например unknown - неизвестное воздействие, у которого неизвестна метка классификации)
    
    * rename_classes - переименовывает некоторые воздействия 
    в соответствии со словарем {"старое_название": "новое_название"}
    
    * truncate_classes - делает одинаковое кол-во воздействий каждого класса. 
    (Downsampling избыточные классы) 
     
    """
    def __init__(self, paths_to_nested: list[str], old_paths: list[str] = None,
                labels_to_delete: list[str] = None, dict_to_rename: dict[str, str] = None,
                balanced_classes: bool = False) -> pd.DataFrame:
        """Инициализатор склейщика данных

        Args:
            paths_to_nested (list[str]): список путей к файлам со вложенной структурой хранения файлов
            old_paths (list[str], optional): список путей к файлам со старой структурой хранения файлов
            labels_to_delete (list[str], optional): список меток для удаления из датасета 
            dict_to_rename (dict[str, str], optional): словарь для переименования меток
            balanced_classes (bool, optional): булева переменная: делать ли undersampling?

        После инициализации класса в аттрибуте data_hdf5 лежит склееный датасет с данными
        из нескольких файлов. 
        """
        self.paths_to_nested = paths_to_nested
        self.old_paths = old_paths
        self.data_getter = TsfreshDatasetTransformer()
        self.list_of_datasets = []
        
        for path in paths_to_nested:
            self.list_of_datasets.append(
                self.data_getter.make_tsfresh_structure_from_nested_directory(path)
            )
        if old_paths:
            for path in old_paths:
                self.list_of_datasets.append(
                    self.data_getter.make_tsfresh_structure_from_simple_directory(path)
                )
        self.data_hdf5, self.label_hdf5 = self.concat_datasets()
        
        if labels_to_delete:
            self.drop_classes(labels_to_delete)
        if dict_to_rename:
            self.rename_classes(dict_to_rename)
        
        self.data_hdf5["id"] = self.rename_idxs(self.data_hdf5["id"])
        self.data_hdf5.reset_index(drop=True, inplace=True)
        self.label_hdf5.reset_index(drop=True, inplace=True)
        if balanced_classes:
            self.truncate_classes()
        
        self.id_to_label = dict(enumerate(self.label_hdf5.unique()))
        self.label_to_id = {i: j for j, i in self.id_to_label.items()}
        self.indexes = self.label_hdf5.index
    def concat_datasets(self):
        """
        Метод для объединения нескольких датасетов из разных
        хранилищ данных

        Returns:
            tuple(pd.DataFrame, pd.Series): Кортеж из df_long, соответствующих меток
        """
        concated_dataframe = pd.DataFrame([])
        concated_labels = []
        current_id = 0
        for (data, label) in self.list_of_datasets:
            data_copy = data.copy()
            data_copy["id"] += current_id
            concated_dataframe = pd.concat([concated_dataframe, data_copy], ignore_index=True)
            current_id = concated_dataframe["id"].iloc[-1] +1
            concated_labels.extend(label)
        return concated_dataframe, pd.Series(concated_labels)

    def rename_idxs(self, array: np.ndarray):
        """Метод устраняет разрывы в массиве чисел, например:
        
        [0, 1, 2, 5, 6, 10, 11]  -> [0, 1, 2, 3, 4, 5, 6]
        
        Этот метод нужен для переименования столбца id в long_df после удаления классов
        """
        a = array.copy()
        a = np.array(a)
        i = 1
        while i < len(a):
            if a[i] - a[i-1] > 1:
                j = i
                constant = a[j]
                while j < len(a) and a[j] == constant:
                    a[j] = a[i-1] + 1
                    j += 1
                i = j
            else:
                i += 1
        if a[0] > 0:
            a -= a[0]
        return a
    
    def drop_classes(self, labels_to_delete: list[str]):
        """
        Удаляет из конечного датасета классы, 
        указанные в labels_to_delete,
        
        Пример: labels_to_delete = ['unknown', 'hit_series']
        """
        mask_drop = self.label_hdf5.isin(labels_to_delete)
        drop_idxs =  self.label_hdf5[mask_drop].index # id of drop labels

        self.data_hdf5.drop(
            index = self.data_hdf5[self.data_hdf5["id"].isin(drop_idxs)].index,
            inplace=True
        )
        self.label_hdf5.drop(
            index = drop_idxs,
            inplace = True
        )
    
    def rename_classes(self, dict_to_rename: dict[str, str]):
        """
        Метод для переименования воздействий в datafame
        Пример dict_to_rename: {'old_name1': 'new_name1', 'old_name2': 'new_name2'}  
        """
        for key, item in dict_to_rename.items():
            self.label_hdf5.replace(key, item, inplace=True)

    def truncate_classes(self):
        """
        Метод для downsampling выборки (для борьбы с дисбалансом классов
        удалаяются объекты преобладающего класса таким образом, чтобы всех
        классов стало одинаковое кол-во)
        """
        idxs_after_truncate = np.array([], dtype=np.uint16) 
        interaction_count = self.label_hdf5.value_counts().min()

        for interaction_name in self.label_hdf5.unique():
            idxs_after_truncate = np.append(idxs_after_truncate, 
                                            self.label_hdf5[self.label_hdf5==interaction_name].
                                            keys().
                                            to_numpy(np.uint16)[:interaction_count])
        idxs_after_truncate.sort()
        self.label_hdf5 = self.label_hdf5.loc[idxs_after_truncate]
        self.data_hdf5 = self.data_hdf5[self.data_hdf5["id"].isin(idxs_after_truncate)]


class TimePreprocessing(BaseEstimator, TransformerMixin):
    """
    Класс, предназначенный для расчета в long_df новых колонок,
    содержащих отфильтрованные сырые сигналы: скользящим средним и
    скользящим СКО. Наследуется от BaseEstimator,
    TransformerMixin, что делает его правильным sklearn-transformer.
    """
    def __init__(self, std_window: int,
                 mean_window:int,
                 normilize:bool) -> pd.DataFrame:
        """
        Инициализатор класса
        Args:
            std_window (int): окно СКО при фильтрации 
            mean_window (int): окно скользящего среднего при фильтрации 
            normilize (bool): нормализовать или нет сырой сигнал перед фильтрами
        """
        self.std_window = std_window
        self.mean_window = mean_window
        self.normilize = normilize
        
    def fit(self, X, y=None):
        """
        Затычка, которая ничего не делает, но нужна для совместимости с sklearn
        """
        return self
    def transform(self, long_df: pd.DataFrame):
        """
        Трансформатор в котором происходит расчет отфильтрованных сигналов

        Args:
            long_df (pd.DataFram): Входной long_df с одним каналом (сырой сигнал)

        Returns:
            long_df (pd.DataFrame): Выходной long_df с тремя каналами:
            (сырой, скользящее СКО, скользящее среднее) 
        """
        if self.normilize:
            stats = long_df.groupby("id")["signal_raw"].agg(["min", "max"])
            long_df = long_df.join(stats, on="id")
            long_df['signal_raw'] = (long_df['signal_raw'] - long_df['min']) / (long_df['max'] - long_df['min'])
            long_df.drop(columns=["min", "max"], inplace=True)
        
        long_df["signal_std"] = (
            long_df.groupby("id")["signal_raw"]
            .rolling(window=self.std_window, min_periods=1, center=True)
            .std().reset_index(level=0, drop=True)
            )
        long_df["signal_mean"] = (
            long_df.groupby("id")["signal_raw"]
            .rolling(window=self.mean_window, min_periods=1, center=True)
            .mean().reset_index(level=0, drop=True)
            )
        return long_df
    def set_output(self, *, transform = None):
        return self

class FreqPreprocessing(BaseEstimator, TransformerMixin):
    """
    Класс, предназначенный для создания из long_df колонки,
    содержащей Фурье, сгрупированное по частоте при помощи адаптивного
    окна скользящего среднего, чем меньше частота, тем меньше размер окна
    Наследуется от BaseEstimator, TransformerMixin, что делает его правильным
    sklearn-transformer.
    """
    def __init__(self, n_bins: int,
                 fs: int)-> pd.DataFrame:
        """
        Инициализатор класса
        Args:
            n_bins (int): кол-во bin-ов в сгруппированном Фурье
            fs (int): частота дискретизации сигнала
        """
        self.n_bins = n_bins
        self.fs = fs
    def binned_fourier(self, signal: np.ndarray) -> np.ndarray:
        """
        Преобразование Фурье сигнала, в котором полученный спектр
        аггрегируется при помощи скользящего окна с увеличивающимся
        окном 
        
        Args:
            signal (np.ndarray): сырой сигнал с интерферометра (во временной области)

        Returns:
            np.ndarray: спектр сигнала 
        """
        N = len(signal)
        fftfreqz = np.fft.rfftfreq(N, 1/self.fs)[1:]
        signal_fft = 2/N * np.abs(np.fft.rfft(signal))[1:]
        bin_edges = np.logspace(np.log2(fftfreqz[0]), np.log2(fftfreqz[-1]), num=self.n_bins+1, base=2)
        inds = np.digitize(fftfreqz, bin_edges) - 1
        binned_fft = np.array([signal_fft[inds == i].mean() for i in range(self.n_bins) if np.any(inds == i)])
        return binned_fft
    def fit(self, X, y=None):
        return self
    def transform(self, long_df: pd.DataFrame) -> pd.DataFrame:
        longFreqDataFrame = pd.DataFrame()
        indexes = long_df["id"].unique()
        result = (
            long_df.groupby("id")
            ["signal_raw"].
            apply(func=lambda x : self.binned_fourier(signal=x),))
            
        for idx in indexes:
            temp_data = result[idx]
            temp_df = pd.DataFrame({
                'id': [int(idx)] * len(temp_data),
                'freq_num': range(len(temp_data)),
                'signal_binned_fft': temp_data
            })
            longFreqDataFrame = pd.concat([longFreqDataFrame, temp_df])
        return longFreqDataFrame.reset_index(drop=True)
    def set_output(self, *, transform = None):
        return self
    
class CustomFeatureAugmenter(FeatureAugmenter):
    """
    Обертка над tsfresh FeatureAugmenter, который, по моему
    не очень удобно устроен из-за set_timeseries_container
    и отсутствия метода set_output.
    """    
    def transform(self, X: pd.DataFrame):
        X_idxs = pd.DataFrame(index=X["id"].unique())
        self.set_timeseries_container(X)
        transformation = super().transform(X_idxs)
        self.set_timeseries_container(None)
        sorted_columns = sorted(transformation.columns)
        return transformation[sorted_columns]
    def set_output(self, *, transform = None):
        return self

class ColumnSorter(BaseEstimator, TransformerMixin):
    """
    Класс-трансформер sklearn который фильтрует колонки в
    датафрейме, нужно для того, чтобы после FeatureUnion 
    колонки всегда шли в одном и том же порядке, иначе модель не будет 
    обучаться, будет выдаваться ошибка.
    """
    def __init__(self):
        pass
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        sorted_columns = sorted(X.columns)
        return X[sorted_columns]
    def set_output(self, *, transform = None):
        return self

class MyCustomFeatures:
    """
    В этом классе описаны признаки в формате tsfresh, которые надо извлечь из
    отфильтрованных сигналов во временной области, а также из сигнала в частотной
    области.
    """
    time_features =  {
            "signal_mean": {
                "permutation_entropy": [
                    {"dimension": 7, "tau": 1},
                    {"dimension": 4, "tau": 1},
                    {"dimension": 5, "tau": 1},
                    {"dimension": 6, "tau": 1},
                    {"dimension": 3, "tau": 1}
                ],
                "number_peaks": [
                    {"n": 1},
                    {"n": 3},
                    {"n": 10},
                    {"n": 5}
                ],
                "ar_coefficient": [
                    {"coeff": 1, "k": 10}
                ],
                "ratio_beyond_r_sigma": [
                    {"r": 0.5},
                    {"r": 3},
                    {"r": 1}
                ],
                "fft_aggregated": [
                    {"aggtype": "variance"},
                    {"aggtype": "centroid"}
                ],
                "absolute_sum_of_changes": None,
                "binned_entropy": [{"max_bins": 10}],
                "lempel_ziv_complexity": [{"bins": 100}],
                "count_below_mean": None,
                "agg_linear_trend": [
                    {"attr": "rvalue", "chunk_len": 50, "f_agg": "min"}
                ]
            },
            "signal_std": {
                "index_mass_quantile": [
                    {"q": 0.9},
                    {"q": 0.7},
                    {"q": 0.8},
                    {"q": 0.6},
                    {"q": 0.4}
                ],
                "quantile": [
                    {"q": 0.1},
                    {"q": 0.7},
                    {"q": 0.8},
                    {"q": 0.9},
                    {"q": 0.3},
                    {"q": 0.4},
                    {"q": 0.6}
                ],
                "minimum": None,
                "count_above_mean": None,
                "variation_coefficient": None,
                "c3": [
                    {"lag": 3},
                    {"lag": 1},
                    {"lag": 2}
                ],
                "sum_values": None,
                "variance": None,
                "absolute_sum_of_changes": None,
                "ar_coefficient": [
                    {"coeff": 1, "k": 10},
                    {"coeff": 0, "k": 10}
                ],
                "standard_deviation": None,
                "permutation_entropy": [
                    {"dimension": 3, "tau": 1},
                    {"dimension": 7, "tau": 1}
                ],
                "root_mean_square": None,
                "agg_linear_trend": [
                    {"attr": "intercept", "chunk_len": 50, "f_agg": "min"},
                    {"attr": "intercept", "chunk_len": 10, "f_agg": "max"},
                    {"attr": "intercept", "chunk_len": 5, "f_agg": "mean"},
                    {"attr": "intercept", "chunk_len": 50, "f_agg": "mean"},
                    {"attr": "intercept", "chunk_len": 5, "f_agg": "max"},
                    {"attr": "intercept", "chunk_len": 5, "f_agg": "min"},
                    {"attr": "stderr", "chunk_len": 50, "f_agg": "mean"}
                ],
                "kurtosis": None,
                "fourier_entropy": [
                    {"bins": 10},
                    {"bins": 100},
                    {"bins": 5},
                    {"bins": 3}
                ],
                "number_peaks": [
                    {"n": 3},
                    {"n": 1}
                ],
                "longest_strike_below_mean": None,
                "ratio_beyond_r_sigma": [
                    {"r": 0.5}
                ],
                "median": None,
                "fft_aggregated": [
                    {"aggtype": "variance"}
                ],
                "lempel_ziv_complexity": [
                    {"bins": 100},
                    {"bins": 5}
                ],
                "skewness": None,
                "range_count": [
                    {"max": 1_000_000_000_000.0, "min": 0},
                    {"max": 1, "min": -1}
                ],
                "cid_ce": [
                    {"normalize": False},
                    {"normalize": True}
                ],
                "benford_correlation": None,
                "binned_entropy": [
                    {"max_bins": 10}
                ]
            }
        }
    freq_features =  {
            "change_quantiles": [
                {"f_agg": "var", "isabs": True, "qh": 1.0, "ql": 0.0},
                {"f_agg": "var", "isabs": True, "qh": 1.0, "ql": 0.4},
                {"f_agg": "var", "isabs": False, "qh": 1.0, "ql": 0.4},
                {"f_agg": "var", "isabs": True, "qh": 1.0, "ql": 0.6},
                {"f_agg": "var", "isabs": False, "qh": 1.0, "ql": 0.0},
                {"f_agg": "var", "isabs": False, "qh": 1.0, "ql": 0.6},
                {"f_agg": "mean", "isabs": True, "qh": 0.4, "ql": 0.0},
                {"f_agg": "mean", "isabs": True, "qh": 0.8, "ql": 0.0},
                {"f_agg": "mean", "isabs": True, "qh": 0.2, "ql": 0.0}
            ],
            "time_reversal_asymmetry_statistic": [
                {"lag": 2},
                {"lag": 3}
            ],
            "quantile": [
                {"q": 0.2},
                {"q": 0.3}
            ],
            "energy_ratio_by_chunks": [
                {"num_segments": 10, "segment_focus": 8},
                {"num_segments": 10, "segment_focus": 9}
            ],
            "agg_linear_trend": [
                {"attr": "intercept", "chunk_len": 5, "f_agg": "var"},
                {"attr": "slope", "chunk_len": 5, "f_agg": "var"},
                {"attr": "stderr", "chunk_len": 5, "f_agg": "var"},
                {"attr": "intercept", "chunk_len": 10, "f_agg": "var"},
                {"attr": "intercept", "chunk_len": 10, "f_agg": "mean"},
                {"attr": "slope", "chunk_len": 50, "f_agg": "max"}
            ],
            "absolute_maximum": None,
            "mean_change": None,
            "index_mass_quantile": [
                {"q": 0.1}
            ],
            "sum_values": None,
            "variance": None
        }