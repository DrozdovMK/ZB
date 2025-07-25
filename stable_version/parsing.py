# В данном скрипте описаны классы с методами для получения данных для обучения.
# При желании можно добавить свой парсер, если будет решение хранить данные
# в каком-то новом формате

import pandas as pd
import numpy as np
import os
import h5py

class SimpleParser():
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

    def get_long_df(self, directory_path: str):
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

class NestedParser():
    def __init__(self):
        pass
    def get_long_df(self, directory_path):
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

class LongJoiner():
    """
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
    def __init__(self, 
                 labels_to_delete: list[str] = None,
                 dict_to_rename: dict[str, str] = None,
                 downsampling: bool = False) -> pd.DataFrame:
        """Инициализатор склейщика данных
        Args:
            labels_to_delete (list[str], optional): список меток для удаления из датасета 
            dict_to_rename (dict[str, str], optional): словарь для переименования меток
            downsampling (bool, optional): булева переменная: делать ли undersampling? 
        """
        self.labels_to_delete = labels_to_delete
        self.dict_to_rename = dict_to_rename
        self.downsampling = downsampling
    
    def concat_datasets(self, datasets_list: list[tuple]) -> tuple:
        """
        Метод для объединения нескольких датасетов из разных
        хранилищ данных, принимает на вход list состоящий из
        longDataFrame, склеивает из в один longDataFrame,
        делает правильный порядок индексов, дропает какие-то
        классы, какие-то переименовывает, может делать downsampling
        для гарантирования что всех классов одинаковое кол-во

        Returns:
            tuple(pd.DataFrame, pd.Series): Кортеж из df_long, соответствующих меток
        """
        concated_dataframe = pd.DataFrame([])
        concated_labels = []
        current_id = 0
        for (data, label) in datasets_list:
            data_copy = data.copy()
            data_copy["id"] += current_id
            concated_dataframe = pd.concat([concated_dataframe, data_copy], ignore_index=True)
            current_id = concated_dataframe["id"].iloc[-1] +1
            concated_labels.extend(label)
        concated_labels = pd.Series(concated_labels)
        
        if self.labels_to_delete:
            concated_dataframe, concated_labels =\
                self.drop_classes(concated_dataframe, concated_labels)
        
        if self.dict_to_rename:
            concated_dataframe, concated_labels =\
                self.rename_classes(concated_dataframe, concated_labels)
        
        concated_dataframe["id"] = self.rename_idxs(concated_dataframe["id"])
        concated_dataframe.reset_index(drop=True, inplace=True)
        concated_labels.reset_index(drop=True, inplace=True)
        
        if self.downsampling:
            concated_dataframe, concated_labels =\
                self.truncate_classes(concated_dataframe, concated_labels)
        
        return concated_dataframe, concated_labels

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
    
    def drop_classes(self, dataframe, labels):
        """
        Удаляет из конечного датасета классы, 
        указанные в self.labels_to_delete,
        
        Пример: self.labels_to_delete = ['unknown', 'hit_series']
        """
        mask_drop = labels.isin(self.labels_to_delete)
        drop_idxs =  labels[mask_drop].index # id of drop labels

        dataframe.drop(
            index = dataframe[dataframe["id"].isin(drop_idxs)].index,
            inplace=True
        )
        labels.drop(
            index = drop_idxs,
            inplace = True
        )
        return dataframe, labels
    
    def rename_classes(self, dataframe, labels):
        """
        Метод для переименования воздействий в datafame
        Пример dict_to_rename: {'old_name1': 'new_name1', 'old_name2': 'new_name2'}  
        """
        for key, item in self.dict_to_rename.items():
            labels.replace(key, item, inplace=True)
        return dataframe, labels

    def truncate_classes(self, dataframe, labels):
        """
        Метод для downsampling выборки (для борьбы с дисбалансом классов
        удалаяются объекты из выборки таким образом чтобы кол-во объектов
        каждого класса стало = кол-ву объектов минимального класса
        
        Например:
        до (900, 400, 150, 300) -> после (150, 150, 150, 150)
        """
        idxs_after_truncate = np.array([], dtype=np.uint16) 
        interaction_count = labels.value_counts().min()

        for interaction_name in labels.unique():
            idxs_after_truncate = np.append(idxs_after_truncate, 
                                            labels[labels==interaction_name].
                                            keys().
                                            to_numpy(np.uint16)[:interaction_count])
        idxs_after_truncate.sort()
        labels = labels.loc[idxs_after_truncate]
        dataframe = dataframe[dataframe["id"].isin(idxs_after_truncate)]
        return dataframe, labels
