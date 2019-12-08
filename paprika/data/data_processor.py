from paprika.data.feed_filter import FilterInterface
from paprika.data.data_channel import DataChannel
from paprika.utils.utils import apply_func, summarize
from paprika.data.feed_filter import TimeFreqFilter, TimePeriod

from haidata.fix_colnames import fix_colnames
from haidata.extract_returns import extract_returns
from functools import partial

import pandas as pd
import functools
from datetime import datetime
import numpy as np
import multiprocessing
from typing import Optional
from absl import logging


class DataProcessor(object):
    MAKE_AVAILABLE_IN_FEEDS = True

    def __init__(self, *args, **kwargs):
        if isinstance(args[0], pd.DataFrame):
            self._data = args[0]
            if DataProcessor.MAKE_AVAILABLE_IN_FEEDS:
                new_table_name = kwargs.get("table_name", None)
                new_table_name = args[1] if new_table_name is None and len(args) > 1 and isinstance(args[1],
                                                                                                    str) else new_table_name
                if new_table_name is not None:
                    DataChannel.upload(self._data, new_table_name, arctic_source_name='feeds', string_format=False)
        elif args and isinstance(args[0], str):
            is_available_in_feeds = args[0] in DataChannel.table_names()
            if kwargs:
                if "arctic_source_name" not in kwargs:
                    kwargs["arctic_source_name"] = 'mdb' if not is_available_in_feeds else 'feeds'
                if "string_format" not in kwargs:
                    kwargs["string_format"] = False
            else:
                if is_available_in_feeds:
                    kwargs = dict({"arctic_source_name": 'feeds', "string_format": False})
                else:
                    kwargs = dict({"arctic_source_name": 'mdb', "string_format": False})
            self._data = DataChannel.download(args[0], *args[1:], **kwargs)
            if DataProcessor.MAKE_AVAILABLE_IN_FEEDS and not is_available_in_feeds:
                DataChannel.upload(self._data, args[0], arctic_source_name='feeds', string_format=False)
        elif args and isinstance(args[0], list):
            # n_processes = multiprocessing.cpu_count()
            # with multiprocessing.Pool(processes=n_processes) as pool:
            #     dps = pool.starmap(DataProcessor, args[0])
            dps = {}
            for symbol in args[0]:
                logging.info(f'Load {symbol} data')
                dp = DataProcessor(symbol)
                dps[symbol] = dp.data
            if len(dps):
                df = pd.concat(dps)
                if isinstance(df.index[0][0], str) and isinstance(df.index[0][1], datetime):
                    df.index.names = ['Symbol', 'Start_Period']
                    if 'Symbol' in df.columns:
                        df.drop(columns=['Symbol'], inplace=True)
                else:
                    raise TypeError(f'Index is {df.index[0]} is not (str, datetime)')
                self._data = df.groupby(['Start_Period', 'Symbol']).first()

        elif "table_name" in kwargs.keys():
            is_available_in_feeds = kwargs["table_name"] in DataChannel.table_names()
            if "arctic_source_name" not in kwargs:
                kwargs["arctic_source_name"] = 'mdb' if not is_available_in_feeds else 'feeds'
            if "string_format" not in kwargs:
                kwargs["string_format"] = False
            self._data = DataChannel.download(**kwargs)
            if DataProcessor.MAKE_AVAILABLE_IN_FEEDS and not is_available_in_feeds:
                DataChannel.upload(self._data, kwargs["table_name"], arctic_source_name='feeds', string_format=False)
        else:
            raise ValueError(f'Unable to interpret DataProcessor arguments: {str(args)} and {str(kwargs)}')
        pass

    def __getattr__(self, item):
        return getattr(self._data, item, None)

    def __call__(self, func, *args, **kwargs):
        if isinstance(func, FilterInterface):
            return DataProcessor(self._data.loc[func.apply(self._data)])
        else:
            ret_value = apply_func(self._data, func, *args, **kwargs)
            if not isinstance(ret_value, type(self._data)):
                raise TypeError(
                    f'Call to DataProcessor should return type {type(self._data)} but returned {type(ret_value)}')
            return DataProcessor(ret_value)

    def __getitem__(self, tuple_of_arguments):
        filter_applied = tuple_of_arguments[0]
        funcs = tuple_of_arguments[1]

        old_return_fixed_indices = filter_applied.return_fixed_indices
        filter_applied.return_fixed_indices = True
        indices_that_exist, fixed_indices = filter_applied.apply(self._data)
        filter_applied.return_fixed_indices = old_return_fixed_indices

        column_names = tuple_of_arguments[2] if len(tuple_of_arguments) > 2 else None
        if column_names is None:
            column_names = list(self._data.columns.values)

        summaries = [summarize(self._data.loc[x[0]:x[1]][column_names], funcs) for
                     x in zip(fixed_indices[:-1], fixed_indices[1:])]

        summary = functools.reduce(lambda df1, df2: pd.concat([df1, df2], ignore_index=False), summaries)
        summary["End_Period"] = fixed_indices[:-1]
        summary["Start_Period"] = fixed_indices[1:]

        summary.set_index('Start_Period', inplace=True)

        if not isinstance(summary, type(self._data)):
            raise TypeError(
                f'Interval Call to DataProcessor should return type {type(self._data)} but returned {type(summary)}')

        # if one wishes to rename the column names that can be done through another __call__
        return DataProcessor(summary)

    @property
    def data(self):
        return self._data.copy()

    @staticmethod
    def _duplicate_col(source_col_name, target_col_name, df):
        df[[target_col_name]] = df[[source_col_name]]
        return df

    @staticmethod
    def _shift(new_column_name, source_column_name, shift_count, df):
        df[[new_column_name]] = df[[source_column_name]].shift(shift_count)
        return df

    @staticmethod
    def first(x):
        return x[0]

    @staticmethod
    def last(x):
        return x[-1]

    # this group of functions are nothing more than convenience functions!!
    # I know, breaks the single interface principle...

    def summarize_intervals(self, time_freq_filter, funcs_list, column_name):
        return self.__getitem__((time_freq_filter, funcs_list, column_name))

    def ohlcv(self,
              time_freq_filter: TimeFreqFilter,
              inplace=False) -> 'DataProcessor':
        if self._data.shape[0] > 0:
            self._data.rename(columns={col: col.upper() for col in self._data.columns}, inplace=True)
            time_freq = f'{time_freq_filter.length}{time_freq_filter.period.value}'
            if all([col in self._data.columns for col in ['PRICE', 'VOLUME']]):
                df_ohlcv = self.ohlcv_from_price(time_freq)
            elif all([col in self._data.columns for col in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']]):
                df_ohlcv = self.ohlcv_from_ohlcv(time_freq)
            else:
                raise ValueError('There is no Price or Volume in data of DataProcessor.')
            df_ohlcv = self.add_return(df_ohlcv)
            if inplace:
                self._data = df_ohlcv
            else:
                return DataProcessor(df_ohlcv)
        else:
            raise ValueError('There is no data in DataProcessor.')

    def add_return(self,
                   df: pd.DataFrame,
                   key: Optional[str] = 'CLOSE') -> pd.DataFrame:
        if df.shape[0] > 0 and key in df.columns:
            df['RETURN'] = df[key].unstack('Symbol').pct_change().stack('Symbol')
            return df
        else:
            raise KeyError(f'No data or Close columns.')

    def ohlcv_from_price(self, time_freq):
        df_ohlcv = self._data["PRICE"].unstack('Symbol').resample(time_freq).ohlc().stack('Symbol')
        df_ohlcv.rename(columns={col: col.upper() for col in df_ohlcv.columns}, inplace=True)
        df_ohlcv['VOLUME'] = self._data['VOLUME'].unstack('Symbol').resample(time_freq).sum().stack('Symbol')
        df_ohlcv['VWAP'] = self._data.unstack('Symbol').resample(time_freq).apply(
            lambda x: (x.PRICE * x.VOLUME).sum() / x.VOLUME.sum()).stack('Symbol')
        df_ohlcv['ADV'] = df_ohlcv['CLOSE'].unstack('Symbol').resample(time_freq).mean().stack('Symbol')

        return df_ohlcv

    def ohlcv_from_ohlcv(self, time_freq):
        df_ohlcv = pd.DataFrame()
        df_ohlcv['OPEN'] = self._data['OPEN'].unstack('Symbol').resample(time_freq).first().stack('Symbol')
        df_ohlcv['CLOSE'] = self._data['CLOSE'].unstack('Symbol').resample(time_freq).last().stack('Symbol')
        df_ohlcv['HIGH'] = self._data['HIGH'].unstack('Symbol').resample(time_freq).max().stack('Symbol')
        df_ohlcv['LOW'] = self._data['LOW'].unstack('Symbol').resample(time_freq).min().stack('Symbol')
        df_ohlcv['VOLUME'] = self._data['VOLUME'].unstack('Symbol').resample(time_freq).sum().stack('Symbol')
        df_ohlcv['VWAP'] = self._data.unstack('Symbol').resample(time_freq).apply(
            lambda x: (x.CLOSE * x.VOLUME).sum() / x.VOLUME.sum()).stack('Symbol')
        df_ohlcv['ADV'] = self._data['CLOSE'].unstack('Symbol').resample(time_freq).mean().stack('Symbol')

        return df_ohlcv

    def time_freq(self, *args, **kwargs):
        return self.__call__(TimeFreqFilter(*args, **kwargs))

    def extract_returns(self, column_name="Price", return_type="LOG_RETURN", new_column_name=None, overwrite=False):
        if new_column_name is None:
            if not overwrite:
                new_column_name = return_type + "_" + column_name
            else:
                new_column_name = column_name
        if new_column_name != column_name and not overwrite:
            self = self.duplicate_column(column_name, new_column_name)
        return self.__call__(extract_returns, {"COLS": new_column_name, "RETURN_TYPE": return_type})

    def between_time(self, start_time, end_time):
        return self.__call__("between_time", start_time, end_time)

    def filter_on_column(self, func, column_name):
        return self.__call__(partial(func, column_name))

    def positive_price(self, price_column="Price"):
        return self.filter_on_column(lambda cn, d: d[d[cn] > 0.0], price_column)

    def index(self, start_index, end_index):
        return self.__call__(partial(lambda x, y, z: z.loc[x:y], start_index, end_index))

    def rename_columns(self, old_names_list, new_names_list):
        return self.__call__(lambda x: x.rename(columns=dict(zip(old_names_list, new_names_list))))

    def duplicate_column(self, source_name, target_name):
        return self.__call__(partial(DataProcessor._duplicate_col, source_name, target_name))

    def shift_to_new_column(self, new_column_name, source_column_name, shift_count):
        return self.__call__(partial(DataProcessor._shift, new_column_name, source_column_name, shift_count))

    @property
    def open(self):
        if 'OPEN' in self._data.columns:
            return self._data['OPEN'].unstack('Symbol')
        else:
            return None

    @property
    def close(self):
        if 'CLOSE' in self._data.columns:
            return self._data['CLOSE'].unstack('Symbol')
        else:
            return None

    @property
    def high(self):
        if 'HIGH' in self._data.columns:
            return self._data['HIGH'].unstack('Symbol')
        else:
            return None

    @property
    def low(self):
        if 'LOW' in self._data.columns:
            return self._data['LOW'].unstack('Symbol')
        else:
            return None

    @property
    def volume(self):
        if 'VOLUME' in self._data.columns:
            return self._data['VOLUME'].unstack('Symbol')
        else:
            return None

    @property
    def vwap(self):
        if 'VWAP' in self._data.columns:
            return self._data['VWAP'].unstack('Symbol')
        else:
            return None

    @property
    def cap(self):
        if 'CAP' in self._data.columns:
            return self._data['CAP'].unstack('Symbol')
        else:
            return None

    @property
    def adv(self, period: str):
        if 'ADV' in self._data.columns:
            return self._data['ADV'].unstack('Symbol')
        else:
            return None

    @property
    def ret(self):
        if 'RETURN' in self._data.columns:
            return self._data['RETURN'].unstack('Symbol')
        else:
            return None
