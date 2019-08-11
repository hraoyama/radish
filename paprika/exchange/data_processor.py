import pandas as pd
import numpy as np
from haidata.fix_colnames import fix_colnames

from paprika.data.feed_filter import *
from paprika.exchange.data_transform import *
from paprika.data.feed import Feed

from paprika.data.data_channel import DataChannel
from paprika.data.feed_filter import FilterInterface


class DataProcessor():
    def __init__(self, data: pd.DataFrame):
        self._data = data
        pass
    
    def __getattr__(self, item):
        return getattr(self._data, item, None)
    
    def __call__(self, function, *args, **kwargs):
        
        if isinstance(function, FilterInterface):
            ret_value = function.apply(*args, **kwargs)
            pass
        else:
            member_func = self.__getattr__(str(function))
            ret_value = member_func(*args, **kwargs) if member_func is not None else function(self._data, *args, **kwargs)
            if not isinstance(ret_value, type(self._data)):
                raise TypeError(
                    f'Call to DataProcessor should return type {type(self._data)} but returned {type(ret_value)}')
            return DataProcessor(ret_value)


if __name__ == "__main__":
    
    # print(DataChannel.table_names(arctic_source_name='mdb'))
    data = DataChannel.download("EUX.FDAX201709.Trade", arctic_source_name='mdb', string_format=False)
    processed_data = DataProcessor(data)
    
    processed_data(fix_colnames, { "CASE" : "UPPER" })
    
