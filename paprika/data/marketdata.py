from collections import OrderedDict, deque
from enum import Enum
import math
from typing import Dict, List

import numpy as np
import pandas as pd
from absl import logging

from paprika.core.api import get_current_frame_timestamp
from paprika.core.context import get_context
from paprika.data.fetcher import HistoricalDataFetcher
from paprika.exchange.constants import (DEFAULT_OHLCV_LIMIT, DEFAULT_ORDER_BOOK_DEPTH,
                                        OHLCV_COLUMN_INDICES, ORDERBOOK_COLUMN_INDICES)
from paprika.utils.time import (current_micros_round_to_seconds,
                                datetime_to_micros, micros_for_frequency,
                                micros_to_datetime)
from paprika.utils.utils import forward_fill_ohlcv


class PriceSource(Enum):
    OHLCV = 0,
    TRADE = 1,
    TICKER = 2,
    ORDERBOOK = 3


class MarketDataBacktest:
    _instance = None

    # Singleton: we share same data across backtest trials.
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, data_fetcher=None):
        config = get_context().config
        self.start_millis = datetime_to_micros(config.start_datetime)
        self.end_millis = datetime_to_micros(config.end_datetime)
        self.interval_millis = micros_for_frequency(config.frequency)

        if not data_fetcher:
            data_fetcher = HistoricalDataFetcher(config.mongodb_host,
                                                 config.redis_host)
        self._data_fetcher = data_fetcher

        self.timestamps: Dict[(str, str, str), np.ndarray] = {}
        self.ohlcvs: Dict[(str, str, str), np.ndarray] = {}
        self.orderbook: Dict[(str, str, str), np.ndarray] = {}
        self.prices: Dict[(str, str, str), np.ndarray] = {}
        self.max_price: Dict[((str, str, str), int), np.ndarray] = {}

    def _timestamp_to_index(self, timestamp, base=None):
        if base is None:
            base = self.start_millis
        assert(timestamp >= base)
        offset = timestamp - base
        assert(offset % self.interval_millis == 0)
        return offset // self.interval_millis

    def _timestamp_to_closet_index(self, timestamp, base=None):
        if base is None:
            base = self.start_millis
        assert(timestamp >= base)
        offset = timestamp - base
        assert(offset % self.interval_millis == 0)
        return offset // self.interval_millis

    def _precalculate_max_price(self, key, window=120):
        prices = self.prices[key]
        max_price = np.full(len(prices), np.nan)
        dq = deque()
        for i in range(len(prices)):
            while dq and prices[i] >= prices[dq[-1]]:
                dq.pop()
            dq.append(i)
            if i >= window and dq and dq[0] <= i - window:
                dq.popleft()
            if i >= window - 1:
                max_price[i] = prices[dq[0]]

        self.max_price[(key, window)] = max_price

    def _precalculate_prices(self, key):
        self.prices[key] = self.ohlcvs[key][OHLCV_COLUMN_INDICES['close'], :]

    def _precalculate(self, key):
        df = self._data_fetcher.fetch_ohlcv(*key)
        timestamps = (df.index.astype(np.int64) // 10**3).values
        values = df.values.T

        # important
        # expected_continuous_indices = pd.date_range(
        #     df.index[0], df.index[-1], freq='T')
        # assert(expected_continuous_indices.equals(df.index))

        # start_millis = max(self.start_millis, timestamps[0])
        # end_millis = min(self.end_millis, timestamps[-1])
        #
        # if end_millis <= start_millis:
        #     self.timestamps[key] = []
        #     self.ohlcvs[key] = []
        #     self.prices[key] = []
        #     self.max_price[key] = []
        #     return

        # logging.info('start_millis, end_millis: %s %s', millis_to_datetime(
        #     timestamps[0]), millis_to_datetime(timestamps[-1]))
        # logging.info('start_millis, end_millis: %s %s', millis_to_datetime(
        #     start_millis), millis_to_datetime(end_millis))

        # start_index = self._timestamp_to_index(start_millis, timestamps[0])
        # end_index = self._timestamp_to_index(end_millis, timestamps[0])

        # logging.info('start_index, end_index: %s %s', start_index, end_index)

        # timestamps = timestamps[start_index:end_index]
        # values = values[:, start_index:end_index]

        # logging.info('timestamps\n %s %s', start_millis, timestamps)
        # logging.info('values\n %s', values)

        # assert(start_millis == timestamps[0])
        # assert(self._timestamp_to_index(end_millis, timestamps[-1]) == 1)

        self.timestamps[key] = timestamps
        self.ohlcvs[key] = values

        self._precalculate_prices(key)

    def _maybe_precalculate(self, key):
        if key not in self.timestamps:
            self._precalculate(key)

    def get_ohlcv(self,
                  source: str,
                  symbol: str,
                  frequency: str,
                  fields: List[str] = None,
                  end_millis: int = None,
                  limit: int = DEFAULT_OHLCV_LIMIT,
                  partial: bool = False) -> (np.ndarray, np.ndarray):

        if partial:
            raise Exception('Partial OHLCV is not supported in backtest.')

        if end_millis is None:
            end_millis = get_current_frame_timestamp()

        key = (source, symbol, frequency)
        self._maybe_precalculate(key)
        timestamps = self.timestamps[key]
        values = self.ohlcvs[key]

        if timestamps.size and end_millis >= timestamps[0]:
            end_index = self._timestamp_to_index(end_millis, timestamps[0])
            start_index = max(end_index - limit, 0)
            timestamps = timestamps[start_index:end_index]
            values = values[:, start_index:end_index]

            if fields:
                field_indices = [
                    OHLCV_COLUMN_INDICES[field] for field in fields
                ]
                values = values[field_indices, :]

            if len(timestamps):
                if len(values) == 1:
                    values = values[0]
                    if len(values) == 1:
                        values = values[0]

                return timestamps, values
            else:
                return [], []
        else:
            return [], []

    def get_prices(self,
                   source: str,
                   symbol: str,
                   timestamp: int = None,
                   limit: int = 1):
        if timestamp is None:
            timestamp = get_current_frame_timestamp()

        price_source = get_context().config.price_source
        if price_source['source'] is PriceSource.OHLCV:
            _, values = self.get_ohlcv(
                source,
                symbol,
                frequency=price_source['frequency'],
                fields=['close'],
                end_millis=timestamp,
                limit=limit)
            return values
        else:
            raise NotImplementedError

    def get_max_price(self,
                      source,
                      symbol,
                      frequency,
                      timestamp: int,
                      window):
        if timestamp is None:
            timestamp = get_current_frame_timestamp()

        key = (source, symbol, frequency)
        self._maybe_precalculate(key)

        if (key, window) not in self.max_price:
            self._precalculate_max_price(key, window)
            # raise ValueError('max limit_price not init for window length %s', window)

        timestamps = self.timestamps[key]
        max_price = self.max_price[(key, window)]
        index = self._timestamp_to_index(timestamp, timestamps[0])

        # "index - 1" to exclude the candle @timestamp, i.e. Look back 'window'
        # candles before timestamp.
        if max_price[index - 1] == np.nan:
            raise ValueError('no enough history data to calculate max limit_price:'
                             ' %s, %s', key, micros_to_datetime(timestamp))
        return max_price[index - 1]

    def get_ticker_price(self, source: str, symbol: str):
        return self.get_prices(source, symbol)

    def get_orderbook(self, source: str,
                      symbol: str,
                      depth: int = DEFAULT_ORDER_BOOK_DEPTH,
                      end_millis: int = None
                      ):

        book_num = len(ORDERBOOK_COLUMN_INDICES)
        book_depth = int(book_num / 4)

        if end_millis is None:
            end_millis = get_current_frame_timestamp()

        symbol = f'{symbol}.OrderBook'
        key = (source, symbol, '')
        if key not in self.timestamps:
            df = self._data_fetcher.fetch_orderbook(*key[0:2])
            if df is None:
                return [], []
            timestamps = (df.index.astype(np.int64) // 10 ** 3).values
            values = df.values.T
            self.timestamps[key] = timestamps
            self.orderbook[key] = values
        else:
            timestamps = self.timestamps[key]
            values = self.orderbook[key]

        if timestamps.size and end_millis >= timestamps[0]:
            closet_index = np.searchsorted(timestamps, end_millis) - 1
            timestamps = timestamps[closet_index]
            values = values[:, closet_index]

            if depth > 0 and depth <= book_depth:
                field_indices = []
                for i in range(book_num):
                    if (i % book_depth) < depth:
                        field_indices.append(i)
                values = values[field_indices]

                return timestamps, values
            elif depth > book_depth:
                return timestamps, values
            else:
                return [], []
        else:
            return [], []

    def get_markets(self, source):
        return self._data_fetcher.load_markets(source)

    def get_tickers(self, source: str):
        markets = self.get_markets(source)
        one_day_mills = micros_for_frequency('1d')
        price_source = get_context().config.price_source
        frequency = price_source['frequency']
        frequency_mills = micros_for_frequency(frequency)
        limit = int(one_day_mills / frequency_mills)
        tickers = {}
        for symbol in markets.keys():
            _, ohlcvs = self.get_ohlcv(source,
                                       symbol,
                                       frequency,
                                       fields=['volume', 'close'],
                                       limit=limit)
            if len(ohlcvs):
                price = ohlcvs[-1]
                volume = np.sum(ohlcvs[0])
                tickers[symbol] = {'symbol': symbol,
                                   'close': price,
                                   'quoteVolume': volume}

        return tickers

    def get_price_volume(self, source, symbol):
        price_source = get_context().config.price_source
        _, ohlcv = self.get_ohlcv(
                source,
                symbol,
                frequency=price_source['frequency'],
                fields=['close', 'volume'],
                limit=1)

        if len(ohlcv) > 1:
            return [ohlcv[0][0], ohlcv[1][0]]
        else:
            return None, None

    def get_market_value(self,
                         source: str,
                         symbol: str,
                         amount: int) -> float:
        _, orderbook = self.get_orderbook(source, symbol)

        if len(orderbook) > 0:
            value = self._calc_value_from_orderbook(orderbook, amount)
        else:
            value = self._calc_value_from_ticker(source, symbol, amount)

        return value

    def _calc_value_from_orderbook(self, orderbook, amount):

        if amount > 0.0:
            prices = orderbook[ORDERBOOK_COLUMN_INDICES['Ask_Px_Lev_0']:ORDERBOOK_COLUMN_INDICES['Bid_Qty_Lev_0']]
            volumes = orderbook[ORDERBOOK_COLUMN_INDICES['Ask_Qty_Lev_0']:]
            value = self._calc_value(prices, volumes, amount)
        elif amount < 0.0:
            prices = orderbook[:ORDERBOOK_COLUMN_INDICES['Ask_Px_Lev_0']]
            volumes = orderbook[ORDERBOOK_COLUMN_INDICES['Bid_Qty_Lev_0']:ORDERBOOK_COLUMN_INDICES['Ask_Qty_Lev_0']]
            value = self._calc_value(prices, volumes, amount)
        else:
            value = 0.0

        return value

    def _calc_value_from_ticker(self, source, symbol, amount):
        price, volume = self.get_price_volume(source, symbol)
        if price is None:
            return 0.
        else:
            amount = min(volume, amount)
            return price * amount

    def _calc_value(self, prices, volumes, amount):
        values = prices * volumes
        volumes = volumes.cumsum()
        index = np.searchsorted(volumes, amount)

        if index == len(volumes):
            value = int(values.sum())
        else:
            _amount = amount - volumes[index]
            _value = _amount * prices[index]
            value = values.cumsum()[index] + _value

        return value

    def get_market_amount(self,
                          source: str,
                          symbol: str,
                          value: float) -> object:
        _, orderbook = self.get_orderbook(source, symbol)

        if len(orderbook) > 0:
            amount = self._calc_amount_from_orderbook(orderbook, value)
        else:
            amount = self._calc_amount_from_ticker(source, symbol, value)

        return amount

    def _calc_amount_from_orderbook(self, orderbook, value):
        if value > 0.0:
            prices = orderbook[ORDERBOOK_COLUMN_INDICES['Ask_Px_Lev_0']:ORDERBOOK_COLUMN_INDICES['Bid_Qty_Lev_0']]
            volumes = orderbook[ORDERBOOK_COLUMN_INDICES['Ask_Qty_Lev_0']:]
            amount = self._calc_amount(prices, volumes, value)
        elif value < 0.0:
            prices = orderbook[:ORDERBOOK_COLUMN_INDICES['Ask_Px_Lev_0']]
            volumes = orderbook[ORDERBOOK_COLUMN_INDICES['Bid_Qty_Lev_0']:ORDERBOOK_COLUMN_INDICES['Ask_Qty_Lev_0']]
            amount = self._calc_amount(prices, volumes, value)
        else:
            amount = 0

        return amount

    def _calc_amount_from_ticker(self, source, symbol, value):
        price, volume = self.get_price_volume(source, symbol)
        if price is None:
            return 0
        else:
            amount = math.floor(value / price)
            return min(volume, amount)

    def _calc_amount(self, prices, volumes, value):
        values = prices * volumes
        values = values.cumsum()
        index = np.searchsorted(values, value)

        if index == len(values):
            return int(volumes.sum())
        else:
            _value = value - values[index]
            _amount = math.floor(_value / prices[index])
            return int(volumes.cumsum()[index] + _amount)