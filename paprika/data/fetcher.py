from paprika.utils.time import millis_to_datetime

from datetime import datetime
from typing import List, Union, Optional

import pandas as pd
import redis
import os
import sys
import logging
import itertools
import functools
import re

from arctic import exceptions
from arctic import Arctic
from arctic import CHUNK_STORE
from arctic.date import DateRange

from .data_type import DataType
from paprika.data.constants import OrderBookColumnName
from paprika.data.constants import TradeColumnName


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

    @staticmethod
    def _scrub_pattern_from_string(patterned_string: str):
        return patterned_string.replace("^", "").replace(".*", "").replace("$", "")

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
                                pattern_list_str: List[str],
                                start_time: Optional[Union[int, datetime]] = None,
                                end_time: Optional[Union[int, datetime]] = None,
                                add_symbol: bool = True,
                                field_columns: List[str] = None,
                                between_times=None,
                                shared_index=False):

        pattern_list = [re.compile(x) for x in pattern_list_str]
        symbols_in_arctic = [symbol_match for symbol_match in self.available_feeds for plist in pattern_list if
                             plist.match(symbol_match)]
        if not symbols_in_arctic:
            # check if somebody temporarily put it in Redis as a test feed
            symbols_in_arctic = [HistoricalDataFetcher._scrub_pattern_from_string(table_name) for table_name in
                                 pattern_list_str]
            dfs = [self._get_redis_table(symbol) for symbol in symbols_in_arctic]
            dfs = [df for df in dfs if df is not None]
            if dfs:
                # symbols_in_arctic =
                return symbols_in_arctic, functools.reduce(
                    lambda df1, df2: pd.concat([df1, df2], ignore_index=False, sort=True), dfs)
            else:
                return [], None

        dfs = list(map(lambda x:
                       self.fetch(symbol=x, fields=field_columns, start_time=start_time, end_time=end_time,
                                  add_symbol=add_symbol),
                       symbols_in_arctic))
        if not shared_index:
            df_all = functools.reduce(lambda df1, df2: pd.concat([df1, df2], ignore_index=False, sort=True), dfs)
        else:
            df_all = pd.DataFrame.drop_duplicates(functools.reduce(lambda df1, df2: pd.concat(
                [df1.loc[df1.index.intersection(df2.index)], df2.loc[df2.index.intersection(df1.index)]],
                ignore_index=False, sort=True), dfs))

        df_all = df_all.between_time(between_times[0], between_times[1]) if between_times is not None else df_all

        # dfAll = functools.reduce(lambda df1, df2: pd.merge(df1, df2, how='outer', left_index=True, right_index=True),dfs)
        # dfAll.sort_index(inplace=True)

        return symbols_in_arctic, df_all

    def fetch(self,
              symbol: str,
              start_time: Optional[Union[int, datetime]] = None,
              end_time: Optional[Union[int, datetime]] = None,
              add_symbol: bool = True,
              fields: List[str] = None):

        if fields is None:
            fields = []
        if start_time is int:
            start_time = millis_to_datetime(start_time)
        if end_time is int:
            end_time = millis_to_datetime(end_time)

        key = symbol
        if start_time and end_time:
            redis_key = key + \
                        f".{start_time.strftime(self.DATETIME_FORMAT)}.{end_time.strftime(self.DATETIME_FORMAT)}." + \
                        ".".join(map(str, fields))
        elif start_time:
            redis_key = key + \
                        f".{start_time.strftime(self.DATETIME_FORMAT)}.None." + \
                        ".".join(map(str, fields))
        elif end_time:
            redis_key = key + \
                        f".None.{end_time.strftime(self.DATETIME_FORMAT)}." + \
                        ".".join(map(str, fields))
        else:
            redis_key = key + \
                        f".None.None." + \
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
                df_result = df_result.loc[~df_result.index.duplicated(keep='first')]
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

    def _get_redis_table(self, table_name):
        msg = self.redis.get(table_name)
        if msg:
            logging.debug(f'Load Redis cache for {table_name}')
            return pd.read_msgpack(msg)
        else:
            return None

    def fetch_orderbook(self,
                        symbol: str,
                        start_time: Optional[Union[int, datetime]] = None,
                        end_time: Optional[Union[int, datetime]] = None,
                        add_symbol: bool = True,
                        fields: List[str] = None):
        return self.fetch(f'{symbol}.OrderBook', start_time, end_time, add_symbol, fields)

    def fetch_trade(self,
                    symbol: str,
                    start_time: Optional[Union[int, datetime]] = None,
                    end_time: Optional[Union[int, datetime]] = None,
                    add_symbol: bool = True,
                    fields: List[str] = None):
        return self.fetch(f'{symbol}.Trade', start_time, end_time, add_symbol, fields)

    def fetch_data_at_timestamp(self,
                                symbol: str,
                                timestamp: Optional[Union[int, datetime]],
                                start_time: Optional[Union[int, datetime]] = None,
                                end_time: Optional[Union[int, datetime]] = None,
                                add_symbol: bool = True,
                                fields: List[str] = None):

        df = self.fetch_orderbook_at_timestamp(symbol, timestamp, start_time, end_time, add_symbol, fields)
        if df is not None:
            return df
        else:
            df = self.fetch_trade_at_timestamp(symbol, timestamp, start_time, end_time, add_symbol, fields)
            return df

    def fetch_orderbook_at_timestamp(self,
                                     symbol: str,
                                     timestamp: Optional[Union[int, datetime]],
                                     start_time: Optional[Union[int, datetime]] = None,
                                     end_time: Optional[Union[int, datetime]] = None,
                                     add_symbol: bool = True,
                                     fields: List[str] = None
                                     ):
        df = self.fetch(f'{symbol}.OrderBook', start_time, end_time, add_symbol, fields)
        if df is not None:
            df_timestamp = df.loc[df.index[df.index.get_loc(timestamp, method='nearest')]]
            return df_timestamp
        else:
            return None

    def fetch_trade_at_timestamp(self,
                                 symbol: str,
                                 timestamp: Optional[Union[int, datetime]],
                                 start_time: Optional[Union[int, datetime]] = None,
                                 end_time: Optional[Union[int, datetime]] = None,
                                 add_symbol: bool = True,
                                 fields: List[str] = None
                                 ):

        df = self.fetch(f'{symbol}.Trade', start_time, end_time, add_symbol, fields)
        if df is not None:
            df_timestamp = df.loc[df.index[df.index.get_loc(timestamp, method='nearest')]]
            return df_timestamp
        else:
            return None

    def fetch_price_at_timestamp(self,
                                 symbol: str,
                                 timestamp: Optional[Union[int, datetime]],
                                 start_time: Optional[Union[int, datetime]] = None,
                                 end_time: Optional[Union[int, datetime]] = None,
                                 add_symbol: bool = True,
                                 fields: List[str] = None):
        df_timestamp = self.fetch_orderbook_at_timestamp(symbol, timestamp, start_time, end_time, add_symbol, fields)
        if df_timestamp is not None:
            bid1 = df_timestamp[OrderBookColumnName.Bid_Px_Lev_0]
            ask1 = df_timestamp[OrderBookColumnName.Ask_Px_Lev_0]
            return (bid1 + ask1) / 2.0
        else:
            df_timestamp = self.fetch_trade_at_timestamp(symbol, timestamp, start_time, end_time, add_symbol, fields)
            return df_timestamp[TradeColumnName.Price]

    def list_of_symbols(self):
        symbols = [symbol.replace('.Trade', '').replace('.OrderBook', '') for symbol in self.available_feeds]
        return list(set(symbols))

    def list_of_symbols_have_orderbook(self):
        symbols = [symbol.replace('.OrderBook', '') for symbol in self.available_feeds if 'OrderBook' in symbol]
        return list(set(symbols))
