from haidata.fix_colnames import fix_colnames
from haidata.extract_returns import extract_returns

from paprika.data.feed_filter import FilterInterface
from paprika.data.feed_filter import TimePeriod, TimeFreqFilter
from paprika.data.data_channel import DataChannel

from pprint import pprint as pp
import numpy as np
import pandas as pd


class DataProcessor():
    def __init__(self, data: pd.DataFrame):
        self._data = data
        pass
    
    def __getattr__(self, item):
        return getattr(self._data, item, None)
    
    def __call__(self, function, *args, **kwargs):
        
        if isinstance(function, FilterInterface):
            return DataProcessor(self._data.loc[function.apply(self._data)])
        else:
            member_func = self.__getattr__(str(function))
            ret_value = member_func(*args, **kwargs) if member_func is not None else function(self._data, *args,
                                                                                              **kwargs)
            if not isinstance(ret_value, type(self._data)):
                raise TypeError(
                    f'Call to DataProcessor should return type {type(self._data)} but returned {type(ret_value)}')
            return DataProcessor(ret_value)
    
    @property
    def data(self):
        return self._data.copy()


if __name__ == "__main__":
    # print(DataChannel.table_names(arctic_source_name='mdb'))
    data = DataChannel.download("EUX.FDAX201709.Trade", arctic_source_name='mdb', string_format=False)
    pp(data.Price['2017-09-15 12:50:49.743080':'2017-09-15 13:00:00.866140'])
    z = DataProcessor(data)(TimeFreqFilter(TimePeriod.MINUTE, 15))(extract_returns,
                                                                {"COLS": "Price", "RETURN_TYPE": "LOG_RETURN"}).data
    pp(z.Price['2017-09-13 21:18:47.488475':'2017-09-15 11:18:44.655347'])
    z2 = DataProcessor(data)(TimeFreqFilter(TimePeriod.HOUR, 1))("between_time", '08:30', '16:30') \
        (extract_returns, {"COLS": "Price", "RETURN_TYPE": "LOG_RETURN"})(fix_colnames, {"CASE": "upper"}).data
    pp(z2.PRICE['2017-09-13 21:18:47.488475':'2017-09-14 11:00'])

    def custom_function(df):
        df[["L1_LOG_RET"]] = df[["PRICE"]].shift(1)
        return df

    z3 = DataProcessor(z2)("between_time", '11:30', '14:00')(custom_function).data
    pp(z3.L1_LOG_RET['2017-09-13 11:00':'2017-09-14 14:00'])
    pp(z3.PRICE['2017-09-13 11:00':'2017-09-14 14:00'])