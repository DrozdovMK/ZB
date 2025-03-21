import numpy as np

class Detector():
    def __init__(self, threshold=2):
        self.threshold = threshold
        self.is_fitted = False
    def fit(self, frames):
        """В рамках этого метода находит параметры для детектора
        frames: np.ndarray - массив шума
        """
        self.noise_std = np.std(frames)
        self.noise_mean = np.mean(frames)
        self.is_fitted = True
    def detect(self, frame):
        assert self.is_fitted, "Detector is not fitted!" 
        frame_std = np.std(frame)
        if frame_std / self.noise_std  > self.threshold:
            return True
        else:
            return False
