import pandas as pd
import logging
from arctic import exceptions, Arctic, CHUNK_STORE
from arctic.date import DateRange

from datetime import datetime, timedelta
import redis
import re
from typing import List, Union, Optional, Tuple
from collections import deque

from paprika.data.data_type import DataType
from paprika.data.constants import OrderBookColumnName, CandleColumnName, TradeColumnName
from paprika.utils.time import millis_to_datetime


class DataChannel:
    redis = redis.Redis('localhost')
    DEFAULT_ARCTIC_HOST = 'localhost'
    PERMANENT_ARCTIC_SOURCE_NAME = 'mdb'
    DEFAULT_ARCTIC_SOURCE_NAME = 'feeds'
    ALL_ARCTIC_SOURCE = (DEFAULT_ARCTIC_SOURCE_NAME,
                         PERMANENT_ARCTIC_SOURCE_NAME
                         )

    DATETIME_FORMAT = "%Y%m%d%H%M%S"
    DATE_FORMAT = "%Y%m%d"
    DATA_INDEX = 'Date'

    TRADE_TIME_SPAN_UNIT = 'H'
    ORDERBOOK_TIME_SPAN_UNIT = 'S'

    @staticmethod
    def library(arctic_source: str = PERMANENT_ARCTIC_SOURCE_NAME,
                arctic_host: str = DEFAULT_ARCTIC_HOST):
        store = Arctic(arctic_host)
        assert arctic_source in store.list_libraries()
        return store[arctic_source]

    @staticmethod
    def name_to_data_type(name: str, data_type: DataType):
        name = name.upper().strip()
        return f'{name}.{str(data_type)}'

    @staticmethod
    def upload_to_permanent(table_name: str,
                            is_overwrite: bool = True
                            ):
        table_name = table_name.upper().strip()
        df = DataChannel.download(table_name, use_redis=True)
        # could have been uploaded to either the temporary 'feeds' or the redis DB
        if df is None:
            df = DataChannel.download(table_name, use_redis=True)
            if df is None:
                raise KeyError(
                    f'{table_name} does not exist in {DataChannel.DEFAULT_ARCTIC_SOURCE_NAME} on ' +
                    f'{DataChannel.DEFAULT_ARCTIC_HOST} or in the REDIS DB')
        else:
            DataChannel.upload(df, table_name, is_overwrite, arctic_source_name='mdb',
                               arctic_host=DataChannel.DEFAULT_ARCTIC_HOST, put_in_redis=False)

    @staticmethod
    def upload_to_redis(data_frame: pd.DataFrame,
                        table_name: str):
        DataChannel.redis.set(table_name, data_frame.to_msgpack(compress='blosc'))
        logging.info(f"Uploaded Redis {table_name} on {DataChannel.DEFAULT_ARCTIC_HOST}")

    @staticmethod
    def clear_redis(keys_list=None):
        keys_to_remove = [x for name in keys_list for x in DataChannel.redis.keys() if
                          name in str(x)] if keys_list is not None else DataChannel.redis.keys()
        for redis_key in keys_to_remove:
            # print("Deleting redis table " + str(redis_key))
            DataChannel.redis.delete(redis_key)

    @staticmethod
    def upload(data_frame: pd.DataFrame,
               table_name: str,
               is_overwrite: bool = True,
               arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME,
               arctic_host: str = DEFAULT_ARCTIC_HOST,
               put_in_redis=True,
               string_format=True):

        arctic = Arctic(arctic_host)
        if arctic_source_name not in arctic.list_libraries():
            arctic.initialize_library(arctic_source_name, lib_type=CHUNK_STORE)

        library = arctic[arctic_source_name]

        table_name = table_name.upper().strip() if string_format else table_name
        if not (table_name in library.list_symbols()):
            library.write(table_name, data_frame)
        elif is_overwrite:
            library.update(table_name, data_frame)
        else:
            library.append(table_name, data_frame)

        logging.info(f"Uploaded Arctic {table_name} to {arctic_source_name} on {arctic_host}")

        if put_in_redis:
            DataChannel.redis.set(table_name, data_frame.to_msgpack(compress='blosc'))
            logging.info(f"Uploaded Redis {table_name} on {DataChannel.DEFAULT_ARCTIC_HOST}")

        return arctic_host, arctic_source_name, table_name

    @staticmethod
    def download(table_name: str,
                 arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME,
                 arctic_host: str = DEFAULT_ARCTIC_HOST,
                 use_redis=True,
                 string_format=True,
                 cascade=True):

        if string_format:
            table_name = table_name.upper().strip()
        msg = DataChannel.redis.get(table_name) if use_redis else None
        if msg:
            logging.debug(f'Load Redis cache for {table_name}')
            return pd.read_msgpack(msg)
        else:
            logging.debug(f'Load Arctic cache for {table_name}')
            arctic = Arctic(arctic_host)
            assert arctic_source_name in arctic.list_libraries()

            library = arctic[arctic_source_name]

            if table_name in library.list_symbols() or not cascade:
                logging.info(f"Read {table_name} inside {arctic_source_name} on {arctic_host}.")
            else:
                library = arctic['mdb']
                logging.info(f"Read {table_name} inside {arctic_source_name} on {arctic_host}.")

            return library.read(table_name)

    @staticmethod
    def delete_table(table_name: str, arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME,
                     arctic_host: str = DEFAULT_ARCTIC_HOST,
                     string_format=True):
        table_name = table_name.upper().strip() if string_format else table_name
        arctic = Arctic(arctic_host)
        assert arctic_source_name in arctic.list_libraries()
        library = arctic[arctic_source_name]
        DataChannel.redis.delete(table_name)
        return library.delete(table_name)

    @staticmethod
    def table_names(arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME, arctic_host: str = DEFAULT_ARCTIC_HOST):
        arctic = Arctic(arctic_host)
        assert arctic_source_name in arctic.list_libraries()
        library = arctic[arctic_source_name]
        return library.list_symbols()

    @staticmethod
    def clear_all_feeds(arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME, arctic_host: str = DEFAULT_ARCTIC_HOST):
        # just to be safe...
        assert arctic_source_name == 'feeds'
        assert arctic_host == 'localhost'
        arctic = Arctic(arctic_host)
        if arctic_source_name not in arctic.list_libraries():
            arctic.initialize_library(arctic_source_name, lib_type=CHUNK_STORE)
        library = arctic[arctic_source_name]
        for table_name in library.list_symbols():
            DataChannel.redis.delete(table_name)
            library.delete(table_name)

    @staticmethod
    def extract_time_span(symbol, time_start, time_delta=timedelta(seconds=300),
                          arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME, arctic_host: str = DEFAULT_ARCTIC_HOST):
        data = DataChannel.download(symbol, arctic_source_name, arctic_host, use_redis=True, string_format=False)
        return_data = data.loc[time_start:(time_start + time_delta)]
        if return_data.shape[0] <= 0:
            return_data = None
        return return_data

    @staticmethod
    def check_register(part_of_symbol_list: List[str],
                       arctic_sources: Tuple[str] = ALL_ARCTIC_SOURCE,
                       arctic_host: str = DEFAULT_ARCTIC_HOST):
        pattern_list = [re.compile(x) for x in part_of_symbol_list]
        symbol_matches = {}
        for arctic_source in arctic_sources:
            library = DataChannel.library(arctic_source, arctic_host)
            symbol_matches[arctic_source] = [symbol_match
                                             for symbol_match in library.list_symbols()
                                             for plist in pattern_list if plist.match(symbol_match)]
        return symbol_matches

    @staticmethod
    def fetch_price_at_timestamp(symbols: List[str],
                                 timestamp: Optional[Union[int, datetime]],
                                 arctic_sources: Tuple[str] = (PERMANENT_ARCTIC_SOURCE_NAME,),
                                 arctic_host: str = DEFAULT_ARCTIC_HOST
                                 ):
        df = DataChannel.fetch_orderbook_at_timestamp(symbols,
                                                      timestamp,
                                                      fields=[OrderBookColumnName.Bid_Px_Lev_0,
                                                              OrderBookColumnName.Ask_Px_Lev_0],
                                                      arctic_sources=arctic_sources,
                                                      arctic_host=arctic_host)
        if df is not None:
            bid1 = df[OrderBookColumnName.Bid_Px_Lev_0]
            ask1 = df[OrderBookColumnName.Ask_Px_Lev_0]
            return (bid1 + ask1) / 2.0
        else:
            df = DataChannel.fetch_trade_at_timestamp(symbols,
                                                      timestamp,
                                                      fields=[TradeColumnName.Price],
                                                      arctic_sources=arctic_sources,
                                                      arctic_host=arctic_host
                                                      )
            if df is not None:
                return df[TradeColumnName.Price]
            else:
                df = DataChannel.fetch_candle_at_timestamp(symbols,
                                                           timestamp,
                                                           fields=[CandleColumnName.Close],
                                                           arctic_sources=arctic_sources,
                                                           arctic_host=arctic_host
                                                           )
                return df[CandleColumnName.Close]

    @staticmethod
    def fetch_orderbook_at_timestamp(symbols: List[str],
                                     timestamp: Union[int, datetime],
                                     fields: Optional[List[str]] = [],
                                     time_span: Optional[int] = 15,
                                     arctic_sources: Tuple[str] = (PERMANENT_ARCTIC_SOURCE_NAME,),
                                     arctic_host: str = DEFAULT_ARCTIC_HOST
                                     ):
        if timestamp is int:
            timestamp = millis_to_datetime(timestamp)
        start = timestamp - pd.to_timedelta(f'{time_span}{DataChannel.ORDERBOOK_TIME_SPAN_UNIT}')
        end = timestamp
        df = DataChannel.fetch_orderbook(symbols,
                                         start,
                                         end,
                                         fields,
                                         arctic_sources,
                                         arctic_host
                                         )

        if df is not None:
            nearest_index = df.index.get_level_values(DataChannel.DATA_INDEX).get_loc(timestamp, method='nearest')
            df_timestamp = df.loc[df.index[nearest_index]]
            return df_timestamp
        else:
            return None

    @staticmethod
    def fetch_trade_at_timestamp(symbols: List[str],
                                 timestamp: Union[int, datetime],
                                 fields: Optional[List[str]] = [],
                                 time_span: Optional[int] = 24,
                                 arctic_sources: Tuple[str] = (PERMANENT_ARCTIC_SOURCE_NAME,),
                                 arctic_host: str = DEFAULT_ARCTIC_HOST
                                 ):
        if timestamp is int:
            timestamp = millis_to_datetime(timestamp)
        start = timestamp - pd.to_timedelta(f'{time_span}{DataChannel.TRADE_TIME_SPAN_UNIT}')
        end = timestamp
        df = DataChannel.fetch_trade(symbols,
                                     start,
                                     end,
                                     fields,
                                     arctic_sources,
                                     arctic_host
                                     )

        if df is not None:
            nearest_index = df.index.get_level_values(DataChannel.DATA_INDEX).get_loc(timestamp, method='nearest')
            df_timestamp = df.loc[df.index[nearest_index]]
            return df_timestamp
        else:
            return None

    @staticmethod
    def fetch_orderbook(symbols: List[str],
                        start: Optional[Union[int, datetime]] = None,
                        end: Optional[Union[int, datetime]] = None,
                        fields: Optional[List[str]] = [],
                        arctic_sources: Tuple[str] = (PERMANENT_ARCTIC_SOURCE_NAME,),
                        arctic_host: str = DEFAULT_ARCTIC_HOST
                        ):
        symbols_in_db = [f'{symbol}.{DataType.ORDERBOOK}' for symbol in symbols]
        return DataChannel.fetch(symbols_in_db,
                                 start,
                                 end,
                                 fields,
                                 arctic_sources,
                                 arctic_host)

    @staticmethod
    def fetch_trade(symbols: List[str],
                    start: Optional[Union[int, datetime]] = None,
                    end: Optional[Union[int, datetime]] = None,
                    fields: Optional[List[str]] = [],
                    arctic_sources: Tuple[str] = (PERMANENT_ARCTIC_SOURCE_NAME,),
                    arctic_host: str = DEFAULT_ARCTIC_HOST
                    ):
        symbols_in_db = [f'{symbol}.{DataType.TRADES}' for symbol in symbols]
        return DataChannel.fetch(symbols_in_db,
                                 start,
                                 end,
                                 fields,
                                 arctic_sources,
                                 arctic_host)

    @staticmethod
    def fetch_candle_at_timestamp(symbols: List[str],
                                  timestamp: Union[int, datetime],
                                  frequency: Optional[str] = '1D',
                                  fields: Optional[List[str]] = [],
                                  arctic_sources: Tuple[str] = (PERMANENT_ARCTIC_SOURCE_NAME,),
                                  arctic_host: str = DEFAULT_ARCTIC_HOST
                                  ):
        if timestamp is int:
            timestamp = millis_to_datetime(timestamp)
        start = timestamp - pd.to_timedelta(frequency)
        end = timestamp
        df = DataChannel.fetch_candle(symbols,
                                      frequency,
                                      start,
                                      end,
                                      fields,
                                      arctic_sources,
                                      arctic_host
                                      )

        if df is not None:
            nearest_index = df.index.get_level_values(DataChannel.DATA_INDEX).get_loc(timestamp, method='nearest')
            return df.loc[df.index[nearest_index]]
        else:
            return None

    @staticmethod
    def fetch_candle(symbols: List[str],
                     frequency: Optional[str] = '1D',
                     start: Optional[Union[int, datetime]] = None,
                     end: Optional[Union[int, datetime]] = None,
                     fields: Optional[List[str]] = [],
                     arctic_sources: Tuple[str] = (PERMANENT_ARCTIC_SOURCE_NAME,),
                     arctic_host: str = DEFAULT_ARCTIC_HOST
                     ):
        symbols_in_db = [f'{symbol}.{frequency}.{DataType.CANDLE}' for symbol in symbols]
        return DataChannel.fetch(symbols_in_db,
                                 start,
                                 end,
                                 fields,
                                 arctic_sources,
                                 arctic_host)

    @staticmethod
    def fetch(symbols: List[str],
              start: Optional[Union[int, datetime]] = None,
              end: Optional[Union[int, datetime]] = None,
              fields: Optional[List[str]] = [],
              arctic_sources: Tuple[str] = (PERMANENT_ARCTIC_SOURCE_NAME,),
              arctic_host: str = DEFAULT_ARCTIC_HOST
              ):

        if start is int:
            start = millis_to_datetime(start)
        if end is int:
            end = millis_to_datetime(end)

        symbols_in_db = DataChannel.check_register(symbols,
                                                   arctic_sources,
                                                   arctic_host)
        dfs = {}
        for arctic_source in arctic_sources:
            for symbol in symbols_in_db[arctic_source]:
                redis_key = DataChannel.get_redis_key(symbol,
                                                      start,
                                                      end,
                                                      fields,
                                                      arctic_sources,
                                                      arctic_host
                                                      )
                if redis_key is not None:
                    msg = DataChannel.redis.get(redis_key)
                    if msg:
                        logging.debug(f'Load Redis cache for {redis_key}')
                        dfs[symbol] = pd.read_msgpack(msg)
                    else:
                        logging.debug(f'Cache not existent for {redis_key}. '
                                      f'Read from mongodb library {DataChannel.PERMANENT_ARCTIC_SOURCE_NAME}.')
                        df = DataChannel.fetch_from_mongodb(symbol,
                                                            start,
                                                            end,
                                                            fields,
                                                            arctic_sources,
                                                            arctic_host)

                        if df is not None:
                            dfs[symbol] = df
                            DataChannel.redis.set(redis_key, df.to_msgpack(compress='blosc'))
        if len(dfs):
            df = pd.concat(dfs)
            df.index.names = ['Symbol', DataChannel.DATA_INDEX]
            # return df.groupby([DataChannel.DATA_INDEX, 'Symbol']).first()
            return df
        else:
            return None

    @staticmethod
    def fetch_from_mongodb(symbol: str,
                           start: Optional[Union[int, datetime]] = None,
                           end: Optional[Union[int, datetime]] = None,
                           fields: Optional[List[str]] = [],
                           arctic_sources: Tuple[str] = ALL_ARCTIC_SOURCE,
                           arctic_host: str = DEFAULT_ARCTIC_HOST):

        for arctic_source in arctic_sources:
            library = DataChannel.library(arctic_source, arctic_host)
            try:
                df = library.read(symbol=symbol, chunk_range=DateRange(start, end), columns=fields)
                df = df.loc[~df.index.duplicated(keep='first')]
                return df
            except exceptions.NoDataFoundException as ndf:
                logging.error(str(ndf))

        return None

    @staticmethod
    def chunk_range(symbol: str,
                    arctic_sources: Tuple[str] = ALL_ARCTIC_SOURCE,
                    arctic_host: str = DEFAULT_ARCTIC_HOST):
        for arctic_source in arctic_sources:
            library = DataChannel.library(arctic_source, arctic_host)
            try:
                result = library.get_chunk_ranges(symbol)
                next(result)
                return library.get_chunk_ranges(symbol)
            except exceptions.NoDataFoundException as ndf:
                logging.error(str(ndf))

        return None

    @staticmethod
    def get_redis_key(symbol,
                      start: Optional[Union[int, datetime]] = None,
                      end: Optional[Union[int, datetime]] = None,
                      fields: Optional[List[str]] = [],
                      arctic_sources: Tuple[str] = ALL_ARCTIC_SOURCE,
                      arctic_host: str = DEFAULT_ARCTIC_HOST
                      ):
        chunk_range = DataChannel.chunk_range(symbol, arctic_sources, arctic_host)
        if chunk_range is not None:
            db_start = pd.to_datetime("".join(map(chr, next(chunk_range)[0])))
            tmp_chunk_range = deque(chunk_range, maxlen=1)
            try:
                db_end = pd.to_datetime("".join(map(chr, tmp_chunk_range.pop()[1])))
            except IndexError:
                db_end = db_start
            if end is None:
                end = db_end
            else:
                end = min(end, db_end)
            if start is None:
                start = db_start
            else:
                start = max(start, db_start)

            redis_key = symbol + \
                        f".{start.strftime(DataChannel.DATETIME_FORMAT)}." \
                        f"{end.strftime(DataChannel.DATETIME_FORMAT)}." + \
                        ".".join(map(str, fields))

            return redis_key
        else:
            return None

    @staticmethod
    def get_redis_table(self, redis_key: str):
        msg = DataChannel.redis.get(redis_key)
        if msg:
            logging.debug(f'Load Redis cache for {redis_key}')
            return pd.read_msgpack(msg)
        else:
            return None
