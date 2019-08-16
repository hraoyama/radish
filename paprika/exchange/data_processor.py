from haidata.fix_colnames import fix_colnames
from haidata.extract_returns import extract_returns

from paprika.data.feed_filter import FilterInterface
from paprika.data.feed_filter import TimePeriod, TimeFreqFilter
from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType
from paprika.utils.record import Timeseries

from pprint import pprint as pp
from functools import partial
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


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
            member_func = self.__getattr__(str(func))
            ret_value = member_func(*args, **kwargs) if member_func is not None else func(self._data, *args,
                                                                                          **kwargs)
            if not isinstance(ret_value, type(self._data)):
                raise TypeError(
                    f'Call to DataProcessor should return type {type(self._data)} but returned {type(ret_value)}')
            return DataProcessor(ret_value)
    
    @property
    def data(self):
        return self._data.copy()


if __name__ == "__main__":
    # to see what is available
    # print(DataChannel.table_names(arctic_source_name='mdb'))
    
    data = DataChannel.download("EUX.FDAX201709.Trade", arctic_source_name='mdb', string_format=False)
    pp(data.Price['2017-09-15 12:55:49.743080':'2017-09-15 13:00:00.866140'])
    
    z = DataProcessor(data) \
        (TimeFreqFilter(TimePeriod.MINUTE, 15)) \
        (extract_returns, {"COLS": "Price", "RETURN_TYPE": "LOG_RETURN"}).data
    pp(z.Price['2017-09-13 23:18:47.488475':'2017-09-15 00:18:44.655347'])
    
    # make source data available from the feeds
    DataProcessor.AVAILABLE_IN_FEEDS = True
    
    z2 = DataProcessor(data, table_name="I_WILL_ACCESS_THIS_LATER") \
        (TimeFreqFilter(TimePeriod.HOUR, 1)) \
        ("between_time", '08:30', '16:30') \
        (extract_returns, {"COLS": "Price", "RETURN_TYPE": "LOG_RETURN"}) \
        (fix_colnames, {"CASE": "upper"}).data
    pp(z2.PRICE['2017-09-13 23:55':'2017-09-14 11:00'])
    
    DataProcessor.AVAILABLE_IN_FEEDS = False
    
    
    def custom_function(df):
        df[["L1_LOG_RET"]] = df[["Price"]].shift(1)
        return df
    
    
    z3 = DataProcessor("I_WILL_ACCESS_THIS_LATER") \
        ("between_time", '11:30', '14:00') \
        (custom_function).data
    pp(z3.L1_LOG_RET['2017-09-13 11:00':'2017-09-14 14:00'])
    pp(z3.Price['2017-09-13 11:00':'2017-09-14 14:00'])
    
    
    def duplicate_col(source_col_name, target_col_name, df):
        df[[target_col_name]] = df[[source_col_name]]
        return df
    
    
    data3 = DataProcessor("EUX.FDAX201709.Trade") \
        ("between_time", '15:59', '16:30') \
        (TimeFreqFilter(TimePeriod.BUSINESS_DAY)) \
        (lambda x: x[x.Price > 0.0]) \
        (partial(duplicate_col, "Price", "LogReturn_Px")) \
        (extract_returns, {"COLS": "LogReturn_Px", "RETURN_TYPE": "LOG_RETURN"}).data
    
    pp(data3['2017-08-16':'2017-09-11'][["Price", "LogReturn_Px"]])
    
    # manipulate new data types
    start = datetime.now()
    end = start + timedelta(days=20)
    num_obs = 1000
    random_dates = start + (end - start) * np.random.random([1, num_obs])
    random_dates.sort()
    random_numbers = np.random.randn(num_obs)
    new_data = Timeseries()
    for ts, val in zip(list(random_dates[0]), random_numbers):
        new_data.append(ts, val)
    DataType.extend('TIMESERIES')
    
    DataProcessor.AVAILABLE_IN_FEEDS = True
    data4 = DataProcessor(new_data.to_dataframe(column_name="observation"), table_name="MyRandomIdentifier") \
        ("between_time", '08:00:00', '09:00:00') \
        (lambda x: x[np.abs(x["observation"]) < 2.0]).data
    pp(data4)

    t1 = DataChannel.download("MyRandomIdentifier", string_format=False)
    pp(t1.head(10))