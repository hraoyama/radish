from collections import OrderedDict
from enum import Enum

DEFAULT_OHLCV_LIMIT = 30
DEFAULT_ORDER_BOOK_DEPTH = 5

DEFAULT_MIN_AMOUNT = 1

OHLCV_COLUMN_INDICES = OrderedDict({
    'open': 0,
    'high': 1,
    'low': 2,
    'close': 3,
    'volume': 4
})

ORDERBOOK_COLUMN_INDICES = OrderedDict({
    'Bid_Px_Lev_0': 0,
    'Bid_Px_Lev_1': 1,
    'Bid_Px_Lev_2': 2,
    'Bid_Px_Lev_3': 3,
    'Bid_Px_Lev_4': 4,
    'Ask_Px_Lev_0': 5,
    'Ask_Px_Lev_1': 6,
    'Ask_Px_Lev_2': 7,
    'Ask_Px_Lev_3': 8,
    'Ask_Px_Lev_4': 9,
    'Bid_Qty_Lev_0': 10,
    'Bid_Qty_Lev_1': 11,
    'Bid_Qty_Lev_2': 12,
    'Bid_Qty_Lev_3': 13,
    'Bid_Qty_Lev_4': 14,
    'Ask_Qty_Lev_0': 15,
    'Ask_Qty_Lev_1': 16,
    'Ask_Qty_Lev_2': 17,
    'Ask_Qty_Lev_3': 18,
    'Ask_Qty_Lev_4': 19,
})


class TradeColumnName(object):
    Price = 'price'
    Qty = 'qty'
    Volume = 'volume'


class CandleColumnName(object):
    Close = 'close'
    Open = 'open'
    High = 'high'
    Low = 'low'
    Volume = 'volume'
    Return = 'return'
    Vwap = 'vwap'


class OrderBookColumnName(object):
    
    Bid_Px_Lev_0 = 'Bid_Px_Lev_0'
    Bid_Px_Lev_1 = 'Bid_Px_Lev_1'
    Bid_Px_Lev_2 = 'Bid_Px_Lev_2'
    Bid_Px_Lev_3 = 'Bid_Px_Lev_3'
    Bid_Px_Lev_4 = 'Bid_Px_Lev_4'
    Ask_Px_Lev_0 = 'Ask_Px_Lev_0'
    Ask_Px_Lev_1 = 'Ask_Px_Lev_1'
    Ask_Px_Lev_2 = 'Ask_Px_Lev_2'
    Ask_Px_Lev_3 = 'Ask_Px_Lev_3'
    Ask_Px_Lev_4 = 'Ask_Px_Lev_4'
    Bid_Qty_Lev_0 = 'Bid_Qty_Lev_0'
    Bid_Qty_Lev_1 = 'Bid_Qty_Lev_1'
    Bid_Qty_Lev_2 = 'Bid_Qty_Lev_2'
    Bid_Qty_Lev_3 = 'Bid_Qty_Lev_3'
    Bid_Qty_Lev_4 = 'Bid_Qty_Lev_4'
    Ask_Qty_Lev_0 = 'Ask_Qty_Lev_0'
    Ask_Qty_Lev_1 = 'Ask_Qty_Lev_1'
    Ask_Qty_Lev_2 = 'Ask_Qty_Lev_2'
    Ask_Qty_Lev_3 = 'Ask_Qty_Lev_3'
    Ask_Qty_Lev_4 = 'Ask_Qty_Lev_4'


class TimePeriod(Enum):
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases
    DAY = 'D'
    BUSINESS_DAY = 'B'
    WEEK = 'W'
    MONTH_END = 'M'
    BUSINESS_MONTH_END = 'BM'
    SEMI_MONTH_END = 'SM'
    QUARTER = 'Q'
    HOUR = 'H'
    BUSINESS_HOUR = 'BH'
    MINUTE = 'T'
    SECOND = 'S'
    MILLISECOND = 'L'
    MICROSECOND = 'U'
    CONTINUOUS = ''