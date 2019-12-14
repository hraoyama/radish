import pandas as pd
import logging
from arctic import exceptions, Arctic, CHUNK_STORE
from arctic.date import DateRange
# import pyarrow as pa

from datetime import datetime, timedelta
import redis
import re
from typing import List, Union, Optional, Tuple, Dict
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
    AVAILABLE_SYMBOLS = {}
    CHUNK_RANGE_CACHE = {}
    LIBRARY_CACHE = {}
    AVAILABLE_LIBRARY = None
    DATETIME_FORMAT = "%Y%m%d%H%M%S"
    DATE_FORMAT = "%Y%m%d"
    DATA_INDEX = 'Date'

    TRADE_TIME_SPAN_UNIT = 'D'
    ORDERBOOK_TIME_SPAN_UNIT = 'S'

    @staticmethod
    def library(arctic_source: str = PERMANENT_ARCTIC_SOURCE_NAME,
                arctic_host: str = DEFAULT_ARCTIC_HOST):
        store = Arctic(arctic_host)
        if DataChannel.AVAILABLE_LIBRARY is None:
            DataChannel.AVAILABLE_LIBRARY = store.list_libraries()
        assert arctic_source in DataChannel.AVAILABLE_LIBRARY
        if arctic_source not in DataChannel.LIBRARY_CACHE.keys():
            DataChannel.LIBRARY_CACHE[arctic_source] = store[arctic_source]
        return DataChannel.LIBRARY_CACHE[arctic_source]

    @staticmethod
    def symbol_add_data_type(symbols: List[str],
                             data_type: DataType,
                             frequency: Optional[str] = '1D'):

        if data_type == DataType.CANDLE:
            symbols_with_data_type = [f'{symbol.upper().strip()}.{frequency}.{data_type}' for symbol in symbols]
        else:
            symbols_with_data_type = [f'{symbol.upper().strip()}.{data_type}' for symbol in symbols]

        return symbols_with_data_type

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

        msg = DataChannel.redis.get(table_name) if use_redis else None
        if msg:
            logging.debug(f'Load Redis cache for {table_name}')
            # return pa.default_serialization_context().deserialize(msg)
            return pd.read_msgpack(msg)
        else:
            if string_format:
                table_name = table_name.upper().strip()
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
    def list_symbols(arctic_sources: Optional[Tuple[str]] = ALL_ARCTIC_SOURCE,
                     arctic_host: Optional[str] = DEFAULT_ARCTIC_HOST,
                     refresh: Optional[bool] = False):
        if len(DataChannel.AVAILABLE_SYMBOLS) == 0 or refresh:
            symbols = {}
            for arctic_source in arctic_sources:
                library = DataChannel.library(arctic_source, arctic_host)
                symbols[arctic_source] = library.list_symbols()
            DataChannel.AVAILABLE_SYMBOLS = symbols
            return symbols
        else:
            return DataChannel.AVAILABLE_SYMBOLS

    @staticmethod
    def check_register(part_of_symbol_list: List[str],
                       arctic_sources: Optional[Tuple[str]] = ALL_ARCTIC_SOURCE,
                       arctic_host: Optional[str] = DEFAULT_ARCTIC_HOST,
                       return_with_type: Optional[bool] = False):
        pattern_list = [re.compile(x) for x in part_of_symbol_list]
        symbol_matches = {}
        symbols = DataChannel.list_symbols(arctic_sources, arctic_host)
        for arctic_source in arctic_sources:
            symbol_matches[arctic_source] = [symbol_match
                                             for symbol_match in symbols[arctic_source]
                                             for plist in pattern_list if plist.match(symbol_match)]
        if return_with_type is False:
            symbol_matches = {arctic_source: DataChannel.symbols_remove_data_type(symbols)
                              for arctic_source, symbols in symbol_matches.items()}

        return symbol_matches

    @staticmethod
    def symbols_remove_data_type(symbols_with_type: List):
        symbols_without_type = []
        for symbol_with_type in symbols_with_type:
            symbol_without_type = DataChannel.symbol_remove_data_type(symbol_with_type)
            if symbol_without_type is not None:
                symbols_without_type.append(symbol_without_type)

        return symbols_without_type

    @staticmethod
    def symbol_remove_data_type(symbol_with_type: str):
        if str(DataType.CANDLE) in symbol_with_type:
            symbol_without_type = symbol_with_type.replace(str(DataType.CANDLE), '')
            symbol_without_type = '.'.join(symbol_without_type.split('.')[0:-2])
        elif str(DataType.ORDERBOOK) in symbol_with_type:
            symbol_without_type = symbol_with_type.replace(f'.{str(DataType.ORDERBOOK)}', '')
        elif str(DataType.TRADES) in symbol_with_type:
            symbol_without_type = symbol_with_type.replace(f'.{str(DataType.TRADES)}', '')
        else:
            symbol_without_type = None
            logging.debug(f'Wrong Data type for {symbol_with_type}.')

        return symbol_without_type

    @staticmethod
    def fetch_price(symbols: List[str],
                    timestamp: Union[int, datetime],
                    data_type: Optional[DataType] = DataType.CANDLE,
                    frequency: Optional[str] = '1D',
                    time_span: Optional[int] = 1,
                    arctic_sources: Optional[Tuple[str]] = ALL_ARCTIC_SOURCE,
                    arctic_host: Optional[str] = DEFAULT_ARCTIC_HOST
                    ):
        df = DataChannel.fetch(symbols,
                               timestamp=timestamp,
                               data_type=DataType.ORDERBOOK,
                               time_span=time_span,
                               fields=[OrderBookColumnName.Bid_Px_Lev_0,
                                       OrderBookColumnName.Ask_Px_Lev_0],
                               arctic_sources=arctic_sources,
                               arctic_host=arctic_host)
        if df is not None:
            df['Price'] = (df[OrderBookColumnName.Bid_Px_Lev_0] + df[OrderBookColumnName.Ask_Px_Lev_0]) / 2
            return df.loc[:, ['Price']]
        else:
            df = DataChannel.fetch(symbols,
                                   timestamp=timestamp,
                                   data_type=DataType.TRADES,
                                   time_span=time_span,
                                   fields=[TradeColumnName.Price],
                                   arctic_sources=arctic_sources,
                                   arctic_host=arctic_host
                                   )
            if df is not None:
                return df
            else:
                df = DataChannel.fetch(symbols,
                                       timestamp=timestamp,
                                       data_type=DataType.CANDLE,
                                       frequency=frequency,
                                       time_span=time_span,
                                       fields=[CandleColumnName.Close],
                                       arctic_sources=arctic_sources,
                                       arctic_host=arctic_host
                                       )
                if df is not None:
                    df.columns = ['Price']
                    return df
                else:
                    logging.info(f'Can not find price for {symbols} at {timestamp}.')
                    return None

    @staticmethod
    def fetch(symbols: List[str],
              data_type: Optional[DataType] = DataType.CANDLE,
              frequency: Optional[str] = '1D',
              timestamp: Optional[Union[int, datetime]] = None,
              start: Optional[Union[int, datetime]] = None,
              end: Optional[Union[int, datetime]] = None,
              time_span: Optional[int] = 1,
              fields: Optional[List[str]] = [],
              arctic_sources: Optional[Tuple[str]] = ALL_ARCTIC_SOURCE,
              arctic_host: Optional[str] = DEFAULT_ARCTIC_HOST,
              return_with_type: Optional[bool] = False
              ):
        start, end = DataChannel._correct_start_end(data_type=data_type,
                                                    frequency=frequency,
                                                    timestamp=timestamp,
                                                    start=start,
                                                    end=end,
                                                    time_span=time_span)

        symbols_with_data_type = DataChannel.symbol_add_data_type(symbols, data_type, frequency)

        symbols_in_db = DataChannel.check_register(symbols_with_data_type,
                                                   arctic_sources,
                                                   arctic_host,
                                                   return_with_type=True)
        dfs = {}
        for arctic_source in arctic_sources:
            for symbol in symbols_in_db[arctic_source]:
                if symbol not in dfs.keys():
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
                            df = pd.read_msgpack(msg)
                            if df.shape[0]:
                                dfs[symbol] = DataChannel.find_closed_df_timestamp(df, timestamp)
                        else:
                            logging.debug(f'Cache not existent for {redis_key}. '
                                          f'Read from mongodb library {DataChannel.PERMANENT_ARCTIC_SOURCE_NAME}.')
                            df = DataChannel.fetch_from_mongodb(symbol,
                                                                start=start,
                                                                end=end,
                                                                fields=fields,
                                                                arctic_sources=arctic_sources,
                                                                arctic_host=arctic_host)

                            if df is not None:
                                if df.shape[0]:
                                    dfs[symbol] = DataChannel.find_closed_df_timestamp(df, timestamp)
                                    DataChannel.redis.set(redis_key, df.to_msgpack(compress='blosc'))
        if len(dfs):
            if return_with_type is False:
                dfs = {DataChannel.symbol_remove_data_type(symbol): df
                       for symbol, df in dfs.items()}
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
                logging.error(f'{str(ndf)} for {start} to {end}')

        return None

    @staticmethod
    def chunk_range(symbol: str,
                    arctic_sources: Tuple[str] = ALL_ARCTIC_SOURCE,
                    arctic_host: str = DEFAULT_ARCTIC_HOST):

        for arctic_source in arctic_sources:
            if arctic_source not in DataChannel.CHUNK_RANGE_CACHE.keys():
                DataChannel.CHUNK_RANGE_CACHE[arctic_source] = {}
            symbols = DataChannel.CHUNK_RANGE_CACHE[arctic_source]
            if symbol in symbols.keys():
                return symbols[symbol]
            else:
                library = DataChannel.library(arctic_source, arctic_host)
                try:
                    chunk_range = library.get_chunk_ranges(symbol)
                    next(chunk_range)
                    chunk_range = library.get_chunk_ranges(symbol)
                    start = pd.to_datetime("".join(map(chr, next(chunk_range)[0])))
                    tmp_chunk_range = deque(chunk_range, maxlen=1)
                    try:
                        end = pd.to_datetime("".join(map(chr, tmp_chunk_range.pop()[1])))
                    except IndexError:
                        end = start
                    symbols[symbol] = [start, end]
                    return start, end
                except exceptions.NoDataFoundException as ndf:
                    logging.error(f'{str(ndf)}: {symbol}')

        return None, None

    @staticmethod
    def get_redis_key(symbol,
                      start: Optional[Union[int, datetime]] = None,
                      end: Optional[Union[int, datetime]] = None,
                      fields: Optional[List[str]] = [],
                      arctic_sources: Tuple[str] = ALL_ARCTIC_SOURCE,
                      arctic_host: str = DEFAULT_ARCTIC_HOST
                      ):
        db_start, db_end = DataChannel.chunk_range(symbol, arctic_sources, arctic_host)
        if db_start is not None:
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

    @staticmethod
    def _correct_start_end(data_type: Optional[DataType] = DataType.CANDLE,
                           frequency: Optional[str] = '1D',
                           timestamp: Union[int, datetime] = None,
                           start: Optional[Union[int, datetime]] = None,
                           end: Optional[Union[int, datetime]] = None,
                           time_span: Optional[int] = 1,
                           ):
        if start is int:
            start = millis_to_datetime(start)
        if end is int:
            end = millis_to_datetime(end)

        if timestamp is not None:
            end = timestamp
            if timestamp is int:
                timestamp = millis_to_datetime(timestamp)
            if data_type == DataType.TRADES:
                start = timestamp - pd.to_timedelta(f'{time_span}{DataChannel.TRADE_TIME_SPAN_UNIT}')
            elif data_type == DataType.ORDERBOOK:
                start = timestamp - pd.to_timedelta(f'{time_span}{DataChannel.ORDERBOOK_TIME_SPAN_UNIT}')
            elif data_type == DataType.CANDLE:
                start = timestamp - pd.to_timedelta(frequency)
            else:
                raise NotImplementedError(f'Not support {data_type} for fetch data at a timestamp now.')

        return start, end

    @staticmethod
    def find_closed_df_timestamp(df: pd.DataFrame,
                                 timestamp: datetime):
        df.index.names = [DataChannel.DATA_INDEX]
        if timestamp is not None:
            nearest_index = df.index.get_level_values(DataChannel.DATA_INDEX).get_loc(timestamp, method='nearest')
            return df.loc[df.index[nearest_index]].to_frame().T
        else:
            return df
