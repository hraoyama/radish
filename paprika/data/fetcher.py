from paprika.utils.time import millis_to_datetime

from datetime import datetime
from typing import List, Union

import pandas as pd
import redis
import os
import sys
import logging
import itertools
import functools
from enum import Enum
import re

from arctic import exceptions
from arctic import Arctic
from arctic import CHUNK_STORE
from arctic.date import DateRange

# lib_path = os.path.abspath(os.path.join(__file__, '..', '..', '..'))
sys.path.append(os.getenv("RADISH_DIR"))


class DataType(Enum):
    ORDERBOOK = 1
    TRADES = 2
    
    def __str__(self):
        if self.value == 1:
            return 'OrderBook'
        elif self.value == 2:
            return 'Trade'
        else:
            return self.name


class DataChannel:
    @staticmethod
    def upload(data_frame: pd.DataFrame,
               table_name: str,
               is_overwrite: bool = True,
               arctic_source_name: str = 'feeds',
               arctic_host: str = 'localhost'):
        
        arctic = Arctic(arctic_host)
        if arctic_source_name not in arctic.list_libraries():
            arctic.initialize_library(arctic_source_name, lib_type=CHUNK_STORE)
        
        library = arctic[arctic_source_name]
        
        if not (table_name in library.list_symbols()):
            library.write(table_name, data_frame)
        elif is_overwrite:
            library.update(table_name, data_frame)
        else:
            library.append(table_name, data_frame)
        
        logging.info(f"Uploaded {table_name} to {arctic_source_name} on {arctic_host}")
        return arctic_host, arctic_source_name, table_name
    
    @staticmethod
    def download(table_name: str,
                 arctic_source_name: str = 'feeds',
                 arctic_host: str = 'localhost'):
        
        arctic = Arctic(arctic_host)
        assert arctic_source_name in arctic.list_libraries()
        
        library = arctic[arctic_source_name]
        logging.info(f"Read {table_name} inside {arctic_source_name} on {arctic_host}.")
        return library.read(table_name)
    
    @staticmethod
    def delete_table(table_name: str, arctic_source_name: str = 'feeds', arctic_host: str = 'localhost'):
        arctic = Arctic(arctic_host)
        assert arctic_source_name in arctic.list_libraries()
        library = arctic[arctic_source_name]
        return library.delete(table_name)
    
    @staticmethod
    def table_names(arctic_source_name: str = 'feeds', arctic_host: str = 'localhost'):
        arctic = Arctic(arctic_host)
        assert arctic_source_name in arctic.list_libraries()
        library = arctic[arctic_source_name]
        return library.list_symbols()
    
    @staticmethod
    def clear_all_feeds(arctic_source_name: str = 'feeds', arctic_host: str = 'localhost'):
        # just to be safe...
        assert arctic_source_name == 'feeds'
        assert arctic_host == 'localhost'
        arctic = Arctic(arctic_host)
        assert arctic_source_name in arctic.list_libraries()
        library = arctic[arctic_source_name]
        for table_name in library.list_symbols():
            library.delete(table_name)


class HistoricalDataFetcher:
    DATETIME_FORMAT = "%Y%m%d%H%M%S"
    DATE_FORMAT = "%Y%m%d"
    
    @staticmethod
    def generate_pattern_list(exchanges: List[str] = None,
                              instruments: List[str] = None,
                              data_types: List[DataType] = None) -> List:
        
        exchanges = ["^.*"] if exchanges is None else list(map(lambda x: ".*" + x.upper().strip() + ".*", exchanges))
        instruments = [".*"] if instruments is None else list(
            map(lambda x: ".*" + x.upper().strip() + ".*", instruments))
        data_types = [str(DataType.ORDERBOOK) + "$"] if data_types is None else list(
            map(lambda x: str(x) + "$", data_types))
        
        return list(map(lambda tup: ".".join(tup), itertools.product(exchanges, instruments, data_types)))
    
    @staticmethod
    def generate_simple_pattern_list(descs: List[str] = None, data_type: DataType = DataType.ORDERBOOK) -> List:
        descs = ["^.*"] if descs is None else list(map(lambda x: ".*" + x.upper().strip() + ".*", descs))
        data_type_pattern = [str(data_type) + "$"]
        return list(map(lambda tup: ".".join(tup), itertools.product(descs, data_type_pattern)))
    
    def __init__(self,
                 arctic_source_name='mdb',
                 arctic_host='localhost',
                 redis_host='localhost'):
        self.arctic = Arctic(arctic_host)
        self.redis = redis.Redis(redis_host)
        if arctic_source_name not in self.arctic.list_libraries():
            self.arctic.initialize_library(arctic_source_name, lib_type=CHUNK_STORE)
        self.library = self.arctic[arctic_source_name]
        self.available_feeds = self.library.list_symbols()
    
    def fetch_from_pattern_list(self,
                                pattern_list: List[str],
                                start_time: Union[int, datetime],
                                end_time: Union[int, datetime],
                                add_symbol: bool = True,
                                field_columns: List[str] = None):
        
        pattern_list = list(map(re.compile, pattern_list))
        symbols_in_arctic = [symbol_match for symbol_match in self.available_feeds for plist in pattern_list if
                             plist.match(symbol_match)]
        if not symbols_in_arctic:
            return symbols_in_arctic, None
        
        dfs = list(map(lambda x:
                       self.fetch(symbol=x, fields=field_columns, start_time=start_time, end_time=end_time,
                                  add_symbol=add_symbol),
                       symbols_in_arctic))
        dfAll = functools.reduce(lambda df1, df2: pd.concat([df1, df2], ignore_index=False, sort=True), dfs)
        # dfAll = functools.reduce(lambda df1, df2: pd.merge(df1, df2, how='outer', left_index=True, right_index=True),dfs)
        # dfAll.sort_index(inplace=True)
        return symbols_in_arctic, dfAll
    
    def fetch(self,
              symbol: str,
              start_time: Union[int, datetime],
              end_time: Union[int, datetime],
              add_symbol: bool = True,
              fields: List[str] = None):
        
        if fields is None:
            fields = []
        if start_time is int:
            start_time = millis_to_datetime(start_time)
        if end_time is int:
            end_time = millis_to_datetime(end_time)
        
        key = symbol
        redis_key = key + \
                    f".{start_time.strftime(self.DATETIME_FORMAT)}.{end_time.strftime(self.DATETIME_FORMAT)}." + \
                    ".".join(map(str, fields))
        msg = self.redis.get(redis_key)
        
        if msg:
            logging.debug(f'Load Redis cache for {redis_key}')
            return pd.read_msgpack(msg)
        else:
            logging.debug(f'Cache not existent for {redis_key}. Read from external source.')
            
            df_result = None
            try:
                df_result = self.library.read(symbol=key, chunk_range=DateRange(start_time, end_time), columns=fields)
                if add_symbol:
                    df_result['Symbol'] = symbol
            except exceptions.NoDataFoundException as ndf:
                logging.error(str(ndf))
                return None
            
            self.redis.set(redis_key, df_result.to_msgpack(compress='blosc'))
            
            return df_result
    
    def get_meta_data(self):
        return dict(zip(self.library.list_symbols(),
                        [self.library.read_metadata(symbol) for symbol in self.library.list_symbols()]))
    
    def fetch_orderbook(self,
                        symbol: str,
                        start_time: Union[int, datetime],
                        end_time: Union[int, datetime],
                        add_symbol: bool = True,
                        fields: List[str] = None):
        return self.fetch(f'{symbol}.OrderBook', start_time, end_time, add_symbol, fields)

