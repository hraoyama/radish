import pytest
from ..fetcher import *

def test_fetcher():

    data_fetcher = HistoricalDataFetcher()
    start_time = datetime(2018, 2, 1)
    end_time = datetime(2018, 2, 2)
    df = data_fetcher.fetch_orderbook('mdb', 'ETF.LU0252634307.OrderBook', '1m', start_time, end_time)
    # print(data_fetcher.fetch_ohlcv(request))
    assert df.shape[0] > 100
