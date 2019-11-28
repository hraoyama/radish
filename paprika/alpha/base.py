import numpy as np
import pandas as pd
from typing import Optional, Tuple, Union, List, Callable
from functools import wraps
from datetime import datetime
from multiprocessing import Pool
import os
import importlib
import sys
import re

from paprika.data.data_processor import DataProcessor
from paprika.alpha.utils import *
from paprika.data.feed_filter import TimeFreqFilter


class Alpha(object):
    alpha_folder = os.path.join(os.path.dirname(__file__), 'alpha')
    alpha_pattern = re.compile('alpha_\w*')

    def __init__(self, alpha_pattern_list: List[str], dp: DataProcessor):
        self._alpha_universe = {}
        self.load_alpha_universe()
        self._dp = dp
        self._alpha = {}
        self.calc_alpha(alpha_pattern_list)

    def __getitem__(self, name):
        return self._alpha[name]

    def load_alpha_universe(self):
        for file in os.listdir(self.alpha_folder):
            if self.alpha_pattern.match(file):
                module_name = f"paprika.alpha.alpha.{file.replace('.py', '')}"
                import_module = importlib.import_module(module_name)
                for _attr in dir(import_module):
                    if self.alpha_pattern.match(_attr):
                        self._alpha_universe[_attr] = getattr(import_module, _attr)

    def calc_alpha(self, alpha_pattern_list: List[str]):
        alpha_patterns = [re.compile(x) for x in alpha_pattern_list]
        use_alpha = {}
        for name, alpha in self._alpha_universe.items():
            for alpha_pattern in alpha_patterns:
                if alpha_pattern.match(name):
                    use_alpha[name] = alpha
                    self._alpha[name] = alpha(self._dp)

        # if len(dfs):
        #     df = pd.concat(dfs)
        #     if isinstance(df.index[0][0], str) and isinstance(df.index[0][1], datetime):
        #         df.index.names = ['Alpha', 'Start_Period']
        #     else:
        #         raise TypeError(f'Index is {df.index[0]} is not (str, datetime)')
        #     return df.groupby(['Start_Period', 'Alpha']).first()

    def list_alpha(self):
        return list(self._alpha.keys())

    def list_alpha_universe(self):
        return list(self._alpha_universe.keys())

    def add_alpha(self, alpha: Callable):
        name = alpha.__name__
        self._alpha_universe[name] = alpha
        self._alpha[name] = alpha(self._dp)

    def reload_alpha_universe(self):
        self._alpha_universe = {}
        self.load_alpha_universe()




def correlation(df: pd.DataFrame,
                other_df: pd.DataFrame,
                period: Union[int, str]) -> pd.DataFrame:
    return df.rolling(period_to_int(period)).corr(other_df.rolling(period_to_int(period)))


def rank(df: pd.DataFrame, axis: Optional[int] = 1) -> pd.DataFrame:
    return df.rank(axis=axis, method='min', ascending=False)


def delay(df: pd.DataFrame, period: Union[int, str]) -> pd.DataFrame:
    return df.shift(period_to_int(period))


def covariance(df: pd.DataFrame,
               other_df: pd.DataFrame,
               period: Union[int, str]) -> pd.Series:
    """
    Cross-Section correlation
    """
    _period = period_str_to_int(period)
    x = df.iloc[:_period, :].values
    y = other_df.iloc[:_period, :].values

    res = np.mean(np.multiply(x, y), axis=0) - np.multiply(np.mean(x, axis=0), np.mean(y, axis=0))
    return pd.Series(res, index=df.columns)


def scale(df: pd.DataFrame, target: float) -> pd.DataFrame:
    return df.mul(df.abs().sum(axis=1).div(target))


def delta(df: pd.DataFrame, period: Union[int, str]) -> pd.DataFrame:
    return df.diff(period_to_int(period))


def ts_min(df: pd.DataFrame, period: Union[int, str]) -> pd.DataFrame:
    return df.rolling(period_to_int(period)).min()


def ts_max(df: pd.DataFrame, period: Union[int, str]) -> pd.DataFrame:
    return df.rolling(period_to_int(period)).max()


def ts_argmin(df: pd.DataFrame, period: Union[int, str]) -> pd.DataFrame:
    return df.rolling(period_to_int(period)).apply(
        lambda x: period_to_int(period) - pd.Series(x)[pd.Series(x) == pd.Series(x).max()].index[0])


def ts_argmax(df: pd.DataFrame, period: Union[int, str]) -> pd.DataFrame:
    return df.rolling(period_to_int(period)).apply(
        lambda x: period_to_int(period) - pd.Series(x)[pd.Series(x) == pd.Series(x).min()].index[0])


def ts_rank(df: pd.DataFrame, period: Union[int, str]) -> pd.DataFrame:
    return df.rolling(period_to_int(period)).apply(
        lambda x: pd.Series(x).rank(axis=0, method='min', ascending=False).iloc[-1])


def ts_sum(df: pd.DataFrame, period: Union[int, str]) -> pd.Series:
    return df.rolling(period_to_int(period)).sum()


def ts_product(df: pd.DataFrame, period: Union[int, str]) -> pd.Series:
    return df.rolling(period_to_int(period)).apply(lambda x: pd.Series(x).product())


def stddev(df: pd.DataFrame, period: Union[int, str]) -> pd.Series:
    return df.rolling(period_to_int(period)).std()