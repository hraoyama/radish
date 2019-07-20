from paprika.data.fetcher import HistoricalDataFetcher
from paprika.data.fetcher import DataUploader
from paprika.data.feed_subscriber import FeedSubscriber
from paprika.data.feed_filter import Filtration

import os
import sys
from datetime import datetime
import logging

print(os.getenv("RADISH_PATH"))
print(os.getenv("PAPRIKA_PATH"))
sys.path.append(os.getenv("RADISH_PATH"))
sys.path.append(os.getenv("RADISH_DIR"))


class FeedSubscription:
    def __init__(self, name: str, start: datetime, end: datetime):
        super().__init__()
        assert end > start
        self.start_datetime = start
        self.end_datetime = end
        self.name = name.upper().strip()
        self.feed_symbols = []
        self.fetcher = HistoricalDataFetcher()
        self.subscribers_dispatch = {}
    
    def __str__(self):
        return_str = f'Feed {self.name} {self.start_datetime.strftime(HistoricalDataFetcher.DATETIME_FORMAT)} to '
        return_str = return_str + f'{self.end_datetime.strftime(HistoricalDataFetcher.DATETIME_FORMAT)} with symbols: '
        return_str = return_str + f'{str(self.feed_symbols)}'
        return return_str
    
    def add_feed(self, list_of_patterns, append=True):
        (matched_symbols, df) = self.fetcher.fetch_from_pattern_list(list_of_patterns,
                                                                     self.start_datetime,
                                                                     self.end_datetime,
                                                                     add_symbol=True)
        DataUploader.upload(df, self.name, is_overwrite=~append)
        self.feed_symbols.extend(matched_symbols)
        self.feed_symbols = list(set(self.feed_symbols))
        pass
    
    def add_subscriber(self, feed_subscriber: FeedSubscriber):
        self.subscribers_dispatch[feed_subscriber.uuid] = []
        pass

    def _get_subsribed_indices(self, filtration: Filtration):
        pass
    

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])
    starting_time = datetime(2017, 5, 31)
    ending_time = datetime(2017, 6, 5)
    my_feed = FeedSubscription("my feed", starting_time, ending_time)
    my_feed.add_feed([".*MTA.IT0001250932.OrderBook.*"])
    print(my_feed.feed_symbols)
    my_feed.add_feed([".*MTA.IT0001250932.OrderBook.*"])
    print(my_feed.feed_symbols)
    my_feed.add_feed([".*EUX.FDAX201709.OrderBook.*"])
    print(my_feed.feed_symbols)
    print(my_feed)
