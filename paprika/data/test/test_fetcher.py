import pytest
from paprika.data.fetcher import *

def test_fetcher():

    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])
    data_fetcher = HistoricalDataFetcher()
    start_time = datetime(2017, 5, 1)
    end_time = datetime(2017, 6, 10)
    meta = data_fetcher.get_meta_data("mdb")
    df = data_fetcher.fetch_orderbook('mdb', 'EUX.FDAX201709', start_time=start_time, end_time=end_time)
    # print(data_fetcher.fetch_ohlcv(request))
    assert df.shape[0] > 100
