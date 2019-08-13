from paprika.data.feed import *

def test_fetcher():

    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])
    data_fetcher = HistoricalDataFetcher()
    starting_time = datetime(2017, 5, 31)
    ending_time = datetime(2017, 6, 5)
    list_of_patterns = HistoricalDataFetcher.generate_simple_pattern_list(['EUX.FDAX201709', 'MTA.IT0001250932'],
                                                                          DataType.ORDERBOOK)
    (matched_symbols, df) = data_fetcher.fetch_from_pattern_list(list_of_patterns, starting_time, ending_time,
                                                                 add_symbol=True)
    assert 'EUX.FDAX201709.OrderBook' in matched_symbols
    assert 'MTA.IT0001250932.OrderBook' in matched_symbols
    assert len(matched_symbols) == 2
    assert 'Ask_Px_Lev_0' in df.columns.values
    assert 'TimeSec' in df.columns.values
    assert df.shape[0] > 100
    assert 'Bid_Px_Lev_4' in df.columns.values
    assert 'Ask_Qty_Lev_4' in df.columns.values
    assert df.shape[1] == 25
    arctic_host, arctic_db, arctic_table_name = DataChannel.upload(df, "_".join(matched_symbols) + "_" +
                                                                   starting_time.strftime(
                                                                       HistoricalDataFetcher.DATE_FORMAT) + "." +
                                                                   ending_time.strftime(
                                                                       HistoricalDataFetcher.DATE_FORMAT),
                                                                   put_in_redis=True,
                                                                   string_format=False)
    assert arctic_table_name == 'EUX.FDAX201709.OrderBook_MTA.IT0001250932.OrderBook_20170531.20170605'
    assert arctic_host == 'localhost'
    assert arctic_db == 'feeds'
    df = data_fetcher.fetch('EUX.FDAX201709.OrderBook', start_time=starting_time, end_time=ending_time, add_symbol=True)
    assert df.shape[1] == 25
    assert df.shape[0] > 100
    assert arctic_table_name in DataChannel.table_names(arctic_db, arctic_host)
    DataChannel.delete_table(arctic_table_name, string_format=False)
    assert arctic_table_name not in DataChannel.table_names(arctic_db, arctic_host)
    DataChannel.clear_all_feeds()
