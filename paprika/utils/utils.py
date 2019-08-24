from datetime import datetime
from math import ceil, floor
from typing import Any, Type

import numpy as np
import pandas as pd

from paprika.utils.time import datetime_to_millis, millis_for_frequency, seconds_for_frequency
from paprika.utils.types import float_type

EPS = 1e-8


# these functions are here just so they have a name...
def first(x):
    return x[0]


def last(x):
    return x[-1]


# https://stackoverflow.com/questions/30399534/shift-elements-in-a-numpy-array
# preallocate empty array and assign slice by chrisaycock
def fast_shift(arr, num, fill_value=np.nan):
    result = np.empty_like(arr)
    if num > 0:
        result[:num] = fill_value
        result[num:] = arr[:-num]
    elif num < 0:
        result[num:] = fill_value
        result[:num] = arr[-num:]
    else:
        result[:] = arr
    return result


def apply_func(data, func, *args, **kwargs):
    member_func = getattr(data, str(func), None) if not isinstance(data, list) else None
    return member_func(*args, **kwargs) if member_func is not None else func(data, *args, **kwargs)


def summarize(data, funcs, **kwargs):
    column_names = None
    if kwargs:
        if 'column_names' in kwargs.keys():
            column_names = [str(x) for x in kwargs['column_names']]
    if column_names is None:
        column_names = [x.__name__ for x in funcs]
    if not column_names or not funcs:
        raise ValueError(
            f'Number of functions ({len(funcs)}) must be >0 and match the number of function names {len(column_names)}')
    
    if len(data) < 1:
        summary_dict = dict([(key, [np.nan]) for key, func in zip(column_names, funcs)])
    else:
        # the functions are independent from each other so do not need to be executed in a loop!
        summary_dict = dict([(key, [apply_func(data, func)]) for key, func in zip(column_names, funcs)])
    
    return pd.DataFrame.from_dict(summary_dict)


def isclose(value1, value2):
    t = float_type()
    return abs(t(value1) - t(value2)) < t(EPS)


def try_parse(to_type: Type, s: Any) -> Any:
    try:
        return to_type(s)
    except BaseException:
        return None


def parse_currency_pair(symbol):
    return symbol.split('/')


def currency_pair(base, quote):
    return f'{base}/{quote}'


def percentage(value, digits=3) -> str:
    return "{0:.{digits}f}%".format(value * 100, digits=digits)


# [start, end] TODO: use [start, end) everywhere.
def forward_fill_ohlcv(df, end=None, frequency=None):
    # TODO: Replace with Pandas resample:
    # https://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.resample.html
    
    if isinstance(df.index, pd.DatetimeIndex):
        timestamps = (df.index.astype(np.int64) // 10 ** 6).to_series()
    else:
        timestamps = df.index.to_series()
    
    start = timestamps.iloc[0]
    
    if end is None:
        end = timestamps.iloc[-1]
    if isinstance(end, datetime):
        end = datetime_to_millis(end)
    
    if frequency is None:
        step = timestamps.diff().value_counts().index[0]
    else:
        step = millis_for_frequency(frequency)
    
    start = ceil(start / step) * step
    end = floor(end / step) * step
    
    filled_index_ms = np.arange(start, end + 1, step, dtype=np.int64)
    filled_index_dt = pd.to_datetime(filled_index_ms, unit='ms')
    
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reindex(filled_index_dt)
    else:
        df = df.reindex(filled_index_ms)
    
    df['close'].fillna(method='ffill', inplace=True)
    df['open'].fillna(df['close'], inplace=True)
    df['high'].fillna(df['close'], inplace=True)
    df['low'].fillna(df['close'], inplace=True)
    df['volume'].fillna(0, inplace=True)
    
    return df


def forward_fill_to_ohlcv(df, end=None, frequency=None):
    df = df.loc[:, ['Price', 'Volume']]
    if end is None:
        end = df.index[-1]
    if end > df.index[-1]:
        end_df = pd.DataFrame(np.nan, index=[end], columns=df.columns)
        df = df.append(end_df)
    
    sec = seconds_for_frequency(frequency)
    tmp = df['Price'].resample(f'{sec}S').ohlc()
    
    tmp['close'].fillna(method='ffill', inplace=True)
    tmp['open'].fillna(tmp['close'], inplace=True)
    tmp['high'].fillna(tmp['close'], inplace=True)
    tmp['low'].fillna(tmp['close'], inplace=True)
    tmp['volume'] = df['Volume'].resample(f'{sec}S').sum()
    
    return tmp
