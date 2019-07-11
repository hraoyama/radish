from datetime import datetime
from math import ceil, floor
from typing import Any, Type

import numpy as np
import pandas as pd

from paprika.utils.time import datetime_to_millis, millis_for_frequency, seconds_for_frequency
from paprika.utils.types import float_type

EPS = 1e-8


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
        timestamps = (df.index.astype(np.int64) // 10**6).to_series()
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

