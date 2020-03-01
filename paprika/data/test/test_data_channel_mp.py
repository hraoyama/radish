from paprika.data.data_channel import DataChannel
import cProfile


def test_data_channel_fetch():
    DataChannel.clear_all_feeds()
    pr = cProfile.Profile()
    pr.enable()
    symbol_patterns = ['SP500.A.*']
    df = DataChannel.fetch(symbol_patterns)
    print(df.shape)
    pr.disable()
    pr.print_stats(sort="cumulative")

    pr = cProfile.Profile()
    pr.enable()
    df = DataChannel.fetch_nmp(symbol_patterns)
    print(df.shape)
    pr.disable()
    pr.print_stats(sort="cumulative")



