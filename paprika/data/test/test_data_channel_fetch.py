from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType

from collections import deque
from datetime import timedelta, datetime
import pandas as pd
import time


def test_data_channel_fetch():
    DataChannel.clear_all_feeds()

    symbol_with_type = 'SP500.COTY.1D.Candle'
    DataChannel.chunk_range(symbol_with_type)
    DataChannel.get_redis_key(symbol_with_type)
    DataChannel.fetch_from_mongodb(symbol_with_type)
    DataChannel.fetch([symbol_with_type])
    symbol = 'SP500.COTY'
    df1 = DataChannel.fetch_candle([symbol], frequency='1D')
    df = DataChannel.fetch_candle_at_timestamp([symbol], df1.index[-10][1])
    df = DataChannel.fetch_price_at_timestamp([symbol], df1.index[-10][1])
    print(df)

    trade_symbols = DataChannel.check_register([f'.*Trade'], feeds_db=False)
    symbol_with_type = trade_symbols[0]
    DataChannel.chunk_range(symbol_with_type)
    DataChannel.get_redis_key(symbol_with_type)
    DataChannel.fetch_from_mongodb(symbol_with_type)
    DataChannel.fetch([symbol_with_type])
    symbol = symbol_with_type.replace('.Trade', '')
    df1 = DataChannel.fetch_trade([symbol])
    df = DataChannel.fetch_trade_at_timestamp([symbol], df1.index[-10][1])
    df = DataChannel.fetch_price_at_timestamp([symbol], df1.index[-10][1])

    print(df)

    orderbook_symbols = DataChannel.check_register([f'.*OrderBook'], feeds_db=False)
    symbol_with_type = orderbook_symbols[1]
    DataChannel.chunk_range(symbol_with_type)
    DataChannel.get_redis_key(symbol_with_type)
    DataChannel.fetch_from_mongodb(symbol_with_type)
    DataChannel.fetch([symbol_with_type])
    symbol = symbol_with_type.replace('.OrderBook', '')
    df1 = DataChannel.fetch_orderbook([symbol])
    df2 = DataChannel.fetch_orderbook_at_timestamp([symbol], df1.index[-10][1])
    df = DataChannel.fetch_price_at_timestamp([symbol], df1.index[10][1])

    print(df)

    # all_orderbook_symbols = DataChannel.check_register([f'.*OrderBook'], feeds_db=True)
    # all_orderbook_symbols2 = DataChannel.check_register([f'.*OrderBook'], feeds_db=False)
    #
    # symbols = all_orderbook_symbols2[0:1]
    #
    # chunk_range = DataChannel.chunk_range(symbols[0])
    # start = pd.to_datetime("".join(map(chr, next(chunk_range)[0])))
    # _chunk_range = deque(chunk_range, maxlen=1)
    # end = pd.to_datetime("".join(map(chr, _chunk_range.pop()[1])))
    #
    # s = time.time()
    # df = DataChannel.fetch_from_mongodb(symbols[0], start=start, end=end)
    # c = time.time() - s
    #
    # s = time.time()
    # df = DataChannel.fetch_from_mongodb(symbols[0], start=start, end=end, fields=['ISIN'])
    # c2 = time.time() - s
    #
    # s = time.time()
    # df = DataChannel.fetch(symbols, start=start, end=end)
    # c3 = time.time() - s
    #
    # symbols = [symbol.replace('.OrderBook', '') for symbol in symbols]
    # df = DataChannel.fetch_orderbook(symbols, fields=['ISIN'])
    # df_t = DataChannel.fetch_orderbook_at_timestamp(symbols, df.index[10][1])
    # p = DataChannel.fetch_price_at_timestamp(symbols, df.index[10][1])
    #
    # trade_symbols = DataChannel.check_register([f'.*Trade'], feeds_db=False)
    #
    # s = time.time()
    # df = DataChannel.fetch_from_mongodb(trade_symbols[0])
    # c = time.time() - s
    #
    # s = time.time()
    # df = DataChannel.fetch_from_mongodb(trade_symbols[0], fields=['ISIN'])
    # c2 = time.time() - s
    #
    # s = time.time()
    # df = DataChannel.fetch(trade_symbols[0:1])
    # c3 = time.time() - s
    #
    # symbols = [symbol.replace('.Trade', '') for symbol in trade_symbols]
    # df = DataChannel.fetch_trade(symbols[0:2], fields=['ISIN'])
    # df_t = DataChannel.fetch_trade_at_timestamp(symbols[0:1], df.index[10][1])
    # p = DataChannel.fetch_price_at_timestamp(symbols[0:1], df.index[10][1])
    # print(p)

    trade_symbols = DataChannel.check_register([f'.*Candle'], feeds_db=False)

    # s = time.time()
    # df = DataChannel.fetch_from_mongodb(trade_symbols[0])
    # c = time.time() - s
    #
    # s = time.time()
    # df = DataChannel.fetch_from_mongodb(trade_symbols[0], fields=['ISIN'])
    # c2 = time.time() - s
    #
    # s = time.time()
    # df = DataChannel.fetch(trade_symbols[0:1])
    # c3 = time.time() - s
    #
    # symbols = [symbol.replace('.Trade', '') for symbol in trade_symbols]
    # df = DataChannel.fetch_trade(symbols[0:2], fields=['ISIN'])
    # df_t = DataChannel.fetch_trade_at_timestamp(symbols[0:1], df.index[10][1])
    # p = DataChannel.fetch_price_at_timestamp(symbols[0:1], df.index[10][1])
    print('ok')




