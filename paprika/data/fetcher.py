from datetime import datetime
from typing import List, Union

import pandas as pd
import redis
import os, sys
# from absl import logging
import logging
from arctic import exceptions
from arctic import Arctic
from arctic.date import DateRange

# lib_path = os.path.abspath(os.path.join(__file__, '..', '..', '..'))
lib_path = os.getenv("RADISH_DIR")
sys.path.append(lib_path)

from paprika.utils.time import millis_to_datetime
from paprika.utils.utils import forward_fill_to_ohlcv


class HistoricalDataFetcher:
    DATETIME_FORMAT = "%Y%m%d%H%M%S"
    
    def __init__(self, arctic_host='localhost', redis_host='localhost'):
        self.arctic = Arctic(arctic_host)
        self.redis = redis.Redis(redis_host)
    
    def fetch_ohlcv(self,
                    source: str,
                    symbol: str,
                    frequency: str,
                    fields: List[str] = None,
                    start_time: Union[int, datetime] = None,
                    end_time: Union[int, datetime] = None):
        if fields is None:
            fields = []
        if start_time is int:
            start_time = millis_to_datetime(start_time)
        if end_time is int:
            end_time = millis_to_datetime(end_time)
        
        # source = f'{source}_ohlcv'
        source = f'{source}'
        # symbol = f'{symbol}_{frequency}'
        symbol = f'{symbol}.Trade'
        
        # key = (source, symbol, frequency, tuple(fields), start_time, end_time)
        key = f'{symbol}.{frequency}.{start_time}.{end_time}'
        msg = self.redis.get(key)
        
        if msg:
            logging.debug('Load Redis cache for %s', key)
            return pd.read_msgpack(msg)
        else:
            logging.debug(
                'Cache not existent for %s. Read from external source.', key)
            
            library = self.arctic[source]
            
            df = library.read(
                symbol=symbol, chunk_range=DateRange(start_time, end_time))
            
            df = forward_fill_to_ohlcv(df, end_time, frequency)
            if fields:
                df = df[fields]
            
            self.redis.set(key, df.to_msgpack(compress='blosc'))
            
            return df
    
    def load_markets(self,
                     source: str):
        # TODO: Use Orderbook for Limit
        library = self.arctic[source]
        symbols = [symbol[0:-6] for symbol in library.list_symbols() if symbol.find('Trade') >= 0]
        markets = {}
        for symbol in symbols:
            quote = 'USD'
            base = symbol
            markets[symbol] = {'base': base, 'quote': quote,
                               'symbol': symbol, 'limits': {'amount': {}}}
        
        return markets
    
    def get_meta_data(self, source: str):
        library = self.arctic[source]
        return dict(zip(library.list_symbols(), [library.read_metadata(symbol) for symbol in library.list_symbols()]))
    
    def fetch_orderbook(self,
                        source: str,
                        symbol: str,
                        fields: List[str] = None,
                        start_time: Union[int, datetime] = None,
                        end_time: Union[int, datetime] = None):
        if fields is None:
            fields = []
        if start_time is int:
            start_time = millis_to_datetime(start_time)
        if end_time is int:
            end_time = millis_to_datetime(end_time)
        
        key = f'{symbol}.OrderBook'  # .{start_time}.{end_time}
        redis_key = key + \
                    f".{start_time.strftime(HistoricalDataFetcher.DATETIME_FORMAT)}.{end_time.strftime(HistoricalDataFetcher.DATETIME_FORMAT)}." + \
                    ".".join(map(str, fields))
        msg = self.redis.get(redis_key)
        
        if msg:
            logging.debug(f'Load Redis cache for {redis_key}')
            return pd.read_msgpack(msg)
        else:
            logging.debug(f'Cache not existent for {redis_key}. Read from external source.')
            library = self.arctic[source]
            
            df = None
            try:
                df = library.read(symbol=key, chunk_range=DateRange(start_time, end_time), columns=fields)
            except exceptions.NoDataFoundException as ndf:
                logging.error(str(ndf))
                return None
            
            from paprika.data.marketdata import ORDERBOOK_COLUMN_INDICES
            df = df[ORDERBOOK_COLUMN_INDICES.keys()]
            
            # if fields:
            #     if all([field in df.columns.values for field in fields]):
            #         df = df[fields]
            
            self.redis.set(redis_key, df.to_msgpack(compress='blosc'))
            
            return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])
    data_fetcher = HistoricalDataFetcher()
    start_time = datetime(2017, 5, 1)
    end_time = datetime(2017, 6, 10)
    meta = data_fetcher.get_meta_data("mdb")
    df = data_fetcher.fetch_orderbook('mdb', 'EUX.FDAX201709', start_time=start_time, end_time=end_time)
    # df = data_fetcher.fetch_orderbook('mdb', 'ETF.LU0252634307')
    # EUX.FDAX201709  # 20170531
    # print(data_fetcher.fetch_ohlcv(request))
    print(df)
