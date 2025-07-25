import os
import h5py
import pandas as pd
import numpy as np
from tsfresh.transformers import FeatureAugmenter
from sklearn.base import BaseEstimator, TransformerMixin

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