import numpy as np
import pandas as pd
from tsfresh.feature_extraction.settings import TimeBasedFCParameters, EfficientFCParameters

# Скользящее СКО для pandas DataFrame, 
# Программа пройдет скользящим СКОпо всем строкам в DataFame
# вход:
#       -signal-pandas DataFram
#       -window-размер окна для скользящего СКО, int
# выход:
#       - pandas DataFrame 
def moving_std(signal, window):
    return signal.rolling(window=window, min_periods=1, center = True, axis='rows').std()

# Скользящее СКО для pandas Series, 
# вход:
#       -signal-pandas Series
#       -window-размер окна для скользящего срднего, int
# выход:
#       - pandas Series 
def moving_std_series(signal, window):
    return signal.rolling(window=window,center = True, min_periods=1).std()

def exp_smooth_df(df, alpha):
    smoothed_df = df.apply(lambda x: x.ewm(alpha, adjust=False).mean(), axis=1)
    return smoothed_df
    
def exp_smooth_np(arr, alpha):
    smoothed_arr = np.zeros(len(arr))
    smoothed_arr[0] = arr[0]
    for i in range(1, len(arr)):
        smoothed_arr[i] = alpha * arr[i] + (1 - alpha) * smoothed_arr[i - 1]
    return smoothed_arr

# Скользящее СКО для pandas DataFrame, 
# Программа пройдет скользящим СКОпо всем фрагментам строк в DataFame, в котоорых находится сигнал
# Полезно для csv файлов, в которых уже есть разметка
# вход:
#       -signal-pandas DataFram
#       -window-размер окна для скользящего СКО, int
# выход:
#       - pandas DataFrame 
def moving_std_df(df, window):
    temp = df
    temp = temp.apply(moving_std, args=[window], axis=1)
    return temp

# Скользящее СКО для numpy array, 
# вход:
#       -signal-pandas DataFram
#       -window-размер окна для скользящего СКО, int
# выход:
#       - numpy array
def moving_std_numpy(signal, window):
    signal = pd.Series(signal)
    return signal.rolling(window=window, min_periods=1, center = True).std().values

#MinMax нормировка
#вход:
#     -signal numpy array
#выход:
#     -numpy array
def MinMax(signal):
    return (signal - np.min(signal))/(np.max(signal) - np.min(signal))

# Программа для выделения канала
# вход:
#       -data-матрица интерферограмм
#       -len_const-период повторения импульсов  
# выход:
#       -центральный канал
def central_chl(data, len_const=10):
    return data[5:10000:len_const]

def get_features():
    sett_answ = TimeBasedFCParameters()
    sett_answ.update(EfficientFCParameters())
    sett_answ.pop('fft_coefficient')
    sett_answ.pop('cwt_coefficients')
    sett_answ.pop('fft_aggregated')
    sett_answ.pop('fourier_entropy')
    return sett_answ



''' Программа для преобразования списка признаков из библиотеки tsfresh
    в словарь для возможности закружать снова в tsfresh 
    Вход:
        -1- Список признаков, который даёт tsfresh 
            Пример: 
                ['0__agg_linear_trend__attr_"intercept"__chunk_len_50__f_agg_"max"',
                 '0__agg_linear_trend__attr_"rvalue"__chunk_len_10__f_agg_"mean"',
                 '0__agg_linear_trend__attr_"slope"__chunk_len_50__f_agg_"max"',
                 '0__ar_coefficient__coeff_2__k_10]
    Выход: 
        -1- Словарь признаков'''

def list2dict_feature(feature_list): 
    feature_dict = {} 
    
    # Обработка параметров признаков    
    def prosessing(feature):
        parametrs_dict = {}
        for parametr in feature:
            parametr = parametr.split('_')
            value = eval(parametr.pop())
            name = "_".join(parametr)
            parametrs_dict[name] = value
        return parametrs_dict     
             
    for feature in feature_list: 
        feature = feature.split('__') 
        del feature[0]
        attr_name =  feature.pop(0)
        if attr_name in tuple(feature_dict.keys()):
            feature_dict[attr_name].append(prosessing(feature))
        else:
            if len(feature) == 0:
                feature_dict[attr_name] = None  
            else:
                feature_dict[attr_name] = [prosessing(feature)]
    return feature_dict



    
    
    
    
