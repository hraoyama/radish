from paprika.data.feed_filter import FilterInterface
from paprika.data.data_channel import DataChannel
from paprika.utils.utils import apply_func, summarize
from paprika.data.feed_filter import TimeFreqFilter, TimePeriod

from haidata.fix_colnames import fix_colnames
from haidata.extract_returns import extract_returns
from functools import partial

import pandas as pd
import functools


class DataProcessor():
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
