from paprika.data.fetcher import HistoricalDataFetcher
from paprika.data.fetcher import DataChannel
from paprika.data.fetcher import DataType
from paprika.data.feed_subscriber import FeedSubscriber
from paprika.data.feed_filter import Filtration

import os
import sys
from datetime import datetime
import logging
import uuid
import functools

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
        
        self.subscribers_dispatch = dict()
        self.subscribers_dispatch[DataType.ORDERBOOK] = {}
        self.subscribers_dispatch[DataType.TRADES] = {}

        self.data_dictionary = dict()
        self.data_dictionary[DataType.ORDERBOOK] = {}
        self.data_dictionary[DataType.TRADES] = {}
    
    def __str__(self):
        return_str = f'Feed {self.name} {self.start_datetime.strftime(HistoricalDataFetcher.DATETIME_FORMAT)} to '
        return_str = return_str + f'{self.end_datetime.strftime(HistoricalDataFetcher.DATETIME_FORMAT)} with symbols: '
        return_str = return_str + f'{str(self.feed_symbols)}'
        return return_str
    
    def add_feed(self, list_of_patterns, data_type: DataType, append=True):
        
        assert data_type in self.data_dictionary.keys()
        
        (matched_symbols, df) = self.fetcher.fetch_from_pattern_list(
            self.fetcher.generate_simple_pattern_list(list_of_patterns, data_type),
            self.start_datetime,
            self.end_datetime,
            add_symbol=True)
        
        uploaded_name = self.name + "_" + str(data_type) + "_" + uuid.uuid4().hex if not self.data_dictionary[
            data_type] else self.data_dictionary[data_type].keys()[0]
        
        DataChannel.upload(df, uploaded_name, is_overwrite=~append)
        self.feed_symbols.extend(matched_symbols)
        self.feed_symbols = list(set(self.feed_symbols))
        
        # necessary to download in case of append
        if append:
            self.data_dictionary[data_type][uploaded_name] = DataChannel.download(uploaded_name)
        
        pass
    
    def add_subscriber(self, feed_subscriber: FeedSubscriber):
        
        # maybe one should leave it up to the subscriber what they want to subscribe to?
        if not feed_subscriber.data_types:
            for data_type_str in self.data_dictionary.keys():
                feed_subscriber.add_data_type(DataType[data_type_str])
        
        assert any(map(lambda x: x in self.data_dictionary.keys(), feed_subscriber.data_types))
        
        for data_type in feed_subscriber.data_types:
            if self.data_dictionary[data_type]:
                for filter_spec in feed_subscriber.filtrations:
                    for subscribed_index_set in self._get_subscribed_indices(filter_spec):
                        self.subscribers_dispatch[subscribed_index_set[0]][subscribed_index_set[1]] = [
                            subscribed_index_set[3]]
    
    def remove_subscriber(self, feed_subscriber: FeedSubscriber):
        unsubscribed_data_types = []
        for data_type in self.subscribers_dispatch.keys():
            if feed_subscriber.uuid in self.subscribers_dispatch[data_type]:
                self.subscribers_dispatch.pop(feed_subscriber.uuid)
                unsubscribed_data_types.append(data_type)
        return unsubscribed_data_types
    
    def clear_subscribers(self):
        self.subscribers_dispatch = {}
    
    def _get_subscribed_indices(self, filtration: Filtration):
        subscribed_indices = []
        for data_type, data_dict in enumerate(self.data_dictionary):
            for uploaded_name, data in enumerate(data_dict):
                filtered_indices = sorted(list(functools.reduce(lambda x1, x2: set(x1).union(set(x2)),
                                                                filtration.apply(data))))
                subscribed_indices.append((data_type, uploaded_name, filtered_indices))
        return subscribed_indices


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
