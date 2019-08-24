from paprika.data.feed_filter import FilterInterface
from paprika.data.data_channel import DataChannel
from paprika.utils.utils import apply_func, fast_shift, summarize

import pandas as pd
import functools


class DataProcessor():
    AVAILABLE_IN_FEEDS = False
    
    def __init__(self, *args, **kwargs):
        if isinstance(args[0], pd.DataFrame):
            self._data = args[0]
            if DataProcessor.AVAILABLE_IN_FEEDS:
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
            if DataProcessor.AVAILABLE_IN_FEEDS:
                DataChannel.upload(self._data, args[0], arctic_source_name='feeds', string_format=False)
        elif "table_name" in kwargs.keys():
            is_available_in_feeds = kwargs["table_name"] in DataChannel.table_names()
            if "arctic_source_name" not in kwargs:
                kwargs["arctic_source_name"] = 'mdb' if not is_available_in_feeds else 'feeds'
            if "string_format" not in kwargs:
                kwargs["string_format"] = False
            self._data = DataChannel.download(**kwargs)
            if DataProcessor.AVAILABLE_IN_FEEDS:
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
        indices_to_grab = filter_applied.apply(self._data)
        indices_shifted = fast_shift(indices_to_grab, -1)
        
        column_names = tuple_of_arguments[2] if len(tuple_of_arguments) > 2 else None
        if column_names is None:
            column_names = list(self._data.columns.values)

        summaries = [summarize(self._data.loc[x[0]:x[1]][column_names], funcs) for
                     x in zip(indices_to_grab[:-1], indices_shifted[:-1])]
        
        summary = functools.reduce(lambda df1, df2: pd.concat([df1, df2], ignore_index=False, sort=True), summaries)
        summary["Start_Interval"] = indices_to_grab[:-1]
        summary["End_Interval"] = indices_shifted[:-1]
        summary.set_index('End_Interval', inplace=True)
        if not isinstance(summary, type(self._data)):
            raise TypeError(
                f'Interval Call to DataProcessor should return type {type(self._data)} but returned {type(summary)}')

        # if one wishes to rename the column names that can be done through another __call__
        return DataProcessor(summary)
    
    @property
    def data(self):
        return self._data.copy()
