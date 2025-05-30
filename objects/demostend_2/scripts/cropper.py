import numpy as np
import sys
from detector import Detector
        
class Cropper():
    """
    Cropper - получает сырые данные порциями, отвечает за детектирование,
    обрезку сигнала
    Выполняемые функции:
    1) Детектирование тревоги из сырого сигнала (см. класс Detector) 
    2) Обрезка сигнала с учетом:
        a) Максимальной длительности сигнала (время после которого обязательно
        нужно выдать классификацию)
        б) Времени охлаждения (время, в течение которого происходит запись
        при отсутствии срабатывания Detector)
        в) Гарантии отступа от начала воздействия.
    """
    def __init__(self, indent_time=500, cooling_time=1000, max_time=10000, detector=Detector()):
        self.indent_time = indent_time # кол-во отсчетов для отступа
        self.max_cooling_time = cooling_time # кол-во отсчетов охлаждения
        self.max_time = max_time # максимальное кол-во отсчетов
        self.detector = detector # Детектор, описанный в классе Detector()
        
        self.cached_frames = np.array([]) # массив для хранения фреймов
        self.alarm_flag = False # флажок сигнализирующий о том что произошла тревога
        self.curr_cooling_time = 0 # текущее время охлаждения
        
    def indent_first_frame(self, frame_current, frame_previous):
        frame_max = np.max(frame_current)
        frame_median = np.median(frame_current)
        frame_min = np.min(frame_current)
        # Определяем индекс пика
        if (frame_max - frame_median) < (frame_median - frame_min):
            peak_idx = frame_current.argmin()
        else:
            peak_idx = frame_current.argmax()
        # Определяем логику склейки/обрезки
        if peak_idx > self.indent_time:
            return frame_current[peak_idx-self.indent_time:]
        else:
            concated_frames = np.concatenate([
                frame_previous[-(self.indent_time - peak_idx):],
                frame_current
            ])
            return concated_frames
    
    def __call__(self, frame):
        if not self.detector.is_fitted:
            self.detector.fit(frame)
        else:
            if len(self.cached_frames) > self.max_time:
                # Событие: длительность тревоги превысила лимит!
                self.alarm_flag = False
                self.curr_cooling_time = 0
                result = self.cached_frames.copy()
                self.cached_frames = np.array([])
                return result
            else:
                # Событие: длительность тревоги не превышена
                if self.detector.detect(frame):
                    # Событие: детектор сработал
                    if self.alarm_flag == False:
                        # Событие: тревога произошла в первый раз
                        indented_frame = self.indent_first_frame(frame, self.last_frame)
                        self.alarm_flag = True
                        self.curr_cooling_time = 0
                        self.cached_frames = np.concatenate([
                                self.cached_frames,
                                indented_frame
                            ])
                    elif self.alarm_flag:
                        # Событие: тревога уже была раньше
                        self.curr_cooling_time = 0
                        self.cached_frames = np.concatenate([
                                self.cached_frames,
                                frame
                            ])
                else:
                    # Событие: детектор НЕ сработал
                    if self.alarm_flag == False:
                        # Событие: тревоги на данный момент нет
                        pass
                    else:
                        # Событие: тревога есть сейчас
                        if self.curr_cooling_time > self.max_cooling_time:
                            # Событие: время охлаждения вышло
                            self.curr_cooling_time = 0
                            self.alarm_flag = False
                            result = self.cached_frames.copy()
                            self.cached_frames = np.array([])
                            return result
                        else:
                            # Событие: время охлаждения еще не закончилось
                            self.curr_cooling_time += len(frame)
                            self.cached_frames = np.concatenate([
                                self.cached_frames,
                                frame
                            ])
        
        self.last_frame = frame
        
        sys.stdout.flush()
        


