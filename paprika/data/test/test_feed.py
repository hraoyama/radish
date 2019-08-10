from paprika.data.data_type import DataType
from paprika.data.feed import Feed, DataChannel
from datetime import datetime
import logging


def test_feed():
    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])
    starting_time = datetime(2017, 5, 31)
    ending_time = datetime(2017, 6, 5)
    my_feed = Feed("my feed", starting_time, ending_time)
    my_feed.set_feed([".*MTA.IT0001250932.*"], DataType.ORDERBOOK)
    print(my_feed.feed_symbols)
    my_feed.set_feed([".*MTA.IT0001250932.*"], DataType.ORDERBOOK)
    print(my_feed.feed_symbols)
    my_feed.set_feed([".*EUX.FDAX201709.*"], DataType.ORDERBOOK)
    print(my_feed.feed_symbols)
    print(my_feed)
    DataChannel.clear_all_feeds()
