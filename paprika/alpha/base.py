import numpy as np
import pandas as pd
from typing import Optional, Tuple, Union
from functools import wraps
from datetime import  datetime

from paprika.data.data_processor import DataProcessor
from paprika.alpha.utils import *
from paprika.data.feed_filter import TimeFreqFilter


def get_alpha_data(symbols: str,
          time_filter: TimeFreqFilter,
          start: Union[str, datetime],
          end: Union[str, datetime],) -> DataProcessor:
    dps = {}
    for symbol in symbols:
        dp = DataProcessor(symbol).index(start, end).ohlcv(time_filter)
        if dp:
            dps[symbol] = dp.data
    df = pd.concat(dps)
    if isinstance(df.index[0][0], str) and isinstance(df.index[0][1], datetime):
        df.index.names = ['Symbol', 'Start_Period']
    else:
        raise TypeError(f'Index is {df.index[0]} is not (str, datetime)')
    df = df.groupby(['Start_Period', 'Symbol']).first()
    return DataProcessor(df)




