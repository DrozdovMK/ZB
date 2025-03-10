
import numpy  as np
import pandas as pd 
# Вход  
# frame         - текущие данные, полученные от Кирила по столбцу(каналу)
# detector_th   - порог для детектора по СКО 
# mstd_th       - порог для детектирования начала воздействия  
# N_wait_frames - количество массивов данных, которое ждет алгос, после последнего массива с тревогой 
# m_std_wdw     - окно скользящего СКО 
# indent         - отступ

# Выход 
# temp          - накопленный массив 
# None          - в случае, если тревоги нет или время охлаждения еще не прошло 
"""
Пример использования 
 from crop import DataStruct 
 .....
 cropper = DataStruct(detector_th, mstd_th, mstd_wdw, indent, N_wait_frames)
 arrays = []
 .....
 (вызываешь объект в любом в начале цикла получения фреймов) 
 с = cropper(frame)
 if c is None:
    pass 
 else: 
    array.append(c)
"""

class Сropper(object): 
    def __init__(self, detector_th = 150, mstd_th = 150, mstd_wdw= 32, indent=500, N_wait_frames=1, max_duration=25000):
        self.detector_th    = detector_th # порог детектора
        self.mstd_th        = mstd_th # порог детектора еще один?
        self.N_wait_frames  = N_wait_frames # время охлаждения
        self.mstd_wdw       = mstd_wdw # окно СКО
        self.indent         = indent # отступ от начала воздействия
        self.concat_in_work = False
        self.frame_in_work  = None
        self.last_frame     = None
        self.counter        = 0
        self.max_duration   = max_duration
    def detector(self, frame):
        return np.std(frame) >= self.detector_th
    
    def moving_std(self, X): 
        X   = pd.Series(X)
        std = X.rolling(window= self.mstd_wdw, min_periods=self.mstd_wdw).std().values 
        std = std[self.mstd_wdw:]
        ind = np.arange(std.shape[0])
        upper_th = ind[std > self.mstd_th] 
        if upper_th.shape[0] == 0:
            return None 
        else: 
            return upper_th[0] + self.mstd_wdw 
        
    def concat_frame(self, ind, frame): 
        # ind - индекс превышения порога
        # frame - фрейм данных
        if ind is not None:    
            if ind < self.indent:
                # логика для записи с отступом если индекс превышения порога < отступа 
                if self.last_frame is not None: 
                    if not self.concat_in_work: 
                        self.frame_in_work = np.concatenate([self.last_frame[-(self.indent-ind):], 
                                                             frame])
                        self.concat_in_work= True
                    else: 
                        self.frame_in_work = np.concatenate([self.frame_in_work,
                                                             frame])
                    self.counter += 1
                else: 
                    pass
                self.signal_len = len(self.frame_in_work)
            else:
                 # логика для записи с отступом если индекс превышения порога > отступа 
                if not self.concat_in_work: 
                    self.frame_in_work = frame[ind - self.indent:] 
                    self.concat_in_work = True
                else: 
                    self.frame_in_work = np.concatenate([self.frame_in_work, 
                                                         frame])
                self.counter += 1
                
        else: 
            pass 
        self.signal_len = len(self.frame_in_work)
        
    def simple_concat_frame(self, frame): 
        self.frame_in_work = np.concatenate([self.frame_in_work,
                                             frame])
        self.counter += 1
        self.signal_len = len(self.frame_in_work)
            
    def __call__(self, frame): 
        if self.detector(frame): 
            self.counter = 0
            ind = self.moving_std(frame)
            self.concat_frame(ind, frame) 
        else: 
            if self.concat_in_work and self.counter <  self.N_wait_frames: 
                if self.signal_len < self.max_duration:
                    self.simple_concat_frame(frame)
                else:
                    temp = self.frame_in_work.copy()
                    self.counter        = 0 
                    self.concat_in_work = False 
                    self.frame_in_work  = None
                    self.last_frame     = frame
                    return temp[:self.max_duration] 
            elif self.concat_in_work and self.counter >=  self.N_wait_frames: 
                self.simple_concat_frame(frame)
                temp = self.frame_in_work.copy()
                self.counter        = 0 
                self.concat_in_work = False 
                self.frame_in_work  = None
                self.last_frame     = frame
                return temp 
        self.last_frame = frame
    