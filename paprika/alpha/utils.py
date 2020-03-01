import numpy as np
import pandas as pd
import math
from typing import Optional, Tuple, Union, List, Callable
from absl import logging


def period_to_int(period: Union[str, int]) -> int:
    if isinstance(period, int):
        return period
    elif isinstance(period, float):
        return math.floor(period)
    elif isinstance(period, str):
        return period_str_to_int(period)
    else:
        raise NotImplementedError


def period_str_to_int(period: str) -> int:
    raise NotImplementedError


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


def returns(df: pd.DataFrame, period: Union[int, str]) -> pd.DataFrame:
    return df / delay(df, period) - 1


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


def ts_sum(df: pd.DataFrame, period: Union[int, str, float]) -> pd.DataFrame:
    return df.rolling(period_to_int(period)).sum()


def ts_mean(df: pd.DataFrame, period: Union[int, str, float]) -> pd.DataFrame:
    return df.rolling(period_to_int(period)).mean()


def ts_product(df: pd.DataFrame, period: Union[int, str, float]) -> pd.DataFrame:
    return df.rolling(period_to_int(period)).apply(lambda x: pd.Series(x).product())


def stddev(df: pd.DataFrame, period: Union[int, str, float]) -> pd.DataFrame:
    return df.rolling(period_to_int(period)).std()


def adv(df: pd.DataFrame, period: Union[int, str, float]) -> pd.DataFrame:
    return df.rolling(period_to_int(period)).std()


def decay_linear(df: pd.DataFrame, period: Union[int, str, float]):
    p = period_to_int(period)
    p_sum = (1 + p) * p / 2
    w = [(i / p_sum) for i in range(1, p + 1)]
    w.reverse()
    w = np.array(w)
    return df.rolling(p).apply(lambda x: sum(x * w))
