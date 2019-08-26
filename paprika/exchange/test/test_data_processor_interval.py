from paprika.data.feed_filter import TimeFreqFilter, TimePeriod
from paprika.exchange.data_processor import DataProcessor
from paprika.utils.utils import first, last

from pprint import pprint as pp
from datetime import datetime

import numpy as np
from functools import partial

from haidata.extract_returns import extract_returns
import matplotlib.pyplot as plt


def shift_colname(column_name, num_shifts, df):
    lag_char = 'L' if num_shifts < 0 else 'F'
    new_column_name = f'{column_name}_{lag_char}{str(np.abs(num_shifts))}'
    df[new_column_name] = df[[column_name]].shift(num_shifts)
    return df


def duplicate_col(source_col_name, target_col_name, df):
    df[[target_col_name]] = df[[source_col_name]]
    return df


def test_data_processor_interval():
    data = DataProcessor("EUX.FDAX201709.Trade") \
        (partial(lambda x, y, z: z.loc[x:y], '2017-06-01 08:00', '2017-06-10 08:00')) \
        ("between_time", '08:15', '16:30') \
        (lambda x: x[x.Price > 0.0]) \
        [TimeFreqFilter(TimePeriod.MINUTE, 5, starting=datetime(2017, 6, 1, 8, 15, 0)),
         [first, np.max, np.min, last, np.median, np.mean, np.std], "Price"] \
        (lambda x: x.rename(columns={'amax': 'HIGH', 'amin': 'LOW', 'mean': 'MEAN',
                                     'median': 'MEDIAN', 'first': 'OPEN', 'last': 'CLOSE', 'std': 'STD'})).data

    pp(data['2017-06-09 12:00':'2017-06-09 13:00'])
    pp(data.HIGH - data.LOW)
    pp(data.columns.values)
    
    data2 = DataProcessor("EUX.FDAX201709.Trade") \
        (partial(lambda x, y, z: z.loc[x:y], '2017-06-01 08:00', '2017-06-10 08:00')) \
        ("between_time", '08:15', '16:30') \
        (lambda x: x[x.Price > 0.0]) \
        [TimeFreqFilter(TimePeriod.MINUTE, 15, starting=datetime(2017, 6, 1, 8, 15, 0)),
         [first, np.max, np.min, last, np.median, np.mean, np.std], "Price"] \
        (lambda x: x.rename(columns={'amax': 'HIGH', 'amin': 'LOW', 'mean': 'MEAN',
                                     'median': 'MEDIAN', 'first': 'OPEN', 'last': 'CLOSE', 'std': 'STD'})) \
        (partial(duplicate_col, "MEAN", "LogReturn_MEAN")) \
        (partial(duplicate_col, "STD", "LogReturn_STD")) \
        (extract_returns, {"COLS": "LogReturn_MEAN,LogReturn_STD", "RETURN_TYPE": "LOG_RETURN"}) \
        (partial(shift_colname, 'LogReturn_MEAN', -1))\
        (partial(shift_colname, 'LogReturn_STD', -1)) \
        (lambda x: x[~np.isnan(x.LogReturn_STD) & ~np.isnan(x.STD) & ~np.isnan(x.LogReturn_STD_L1)]).data
    
    pp(data2.columns.values)
    pp(data2.head(10))
    
    plt.scatter(data2.STD.values[data2.STD > 0.0], data2.LogReturn_MEAN_L1.values[data2.STD > 0.0], alpha=0.5)
    plt.title('Scatter plot of derived data at intervals')
    plt.xlabel('data2.STD')
    plt.ylabel('data2.LogReturn_MEAN_L1')
    plt.show()
