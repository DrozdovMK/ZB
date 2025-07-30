import numpy as np

class Detector():
    """
    Класс, описывающий детектор для определения наличия тревоги
    """
    def __init__(self, threshold):
        """
        threshold (int): Порог тревоги. Допустимое отношение СКО
        внутри фрейма относительно шума устройства
        is_fitted (bool): Настроен ли детектор? 
        (В первую секунду работы системы не настроен, потом находится
        шум устройства, настраивается)
        """
        self.threshold = threshold
        self.is_fitted = False
    def fit(self, frames):
        """
        В рамках этого метода находится значение СКО шума устройства
        frames: np.ndarray - массив шума
        """
        self.noise_std = np.std(frames)
        self.noise_mean = np.mean(frames)
        self.is_fitted = True
    def detect(self, frame) -> bool:
        """
        Метод проверки наличия тревоги
        Args:
            frame (np.ndarray): фрейм данных от прибора (сырой сигнал)
        Returns:
            bool: Есть ли тревога?
        """
        assert self.is_fitted, "Detector is not fitted!" 
        frame_std = np.std(frame)
        if frame_std / self.noise_std  > self.threshold:
            return True
        else:
            return False