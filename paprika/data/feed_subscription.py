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
        self.data_dictionary = {'OrderBook': {}, 'Trade': {}}
    
    def __str__(self):
        return_str = f'Feed {self.name} {self.start_datetime.strftime(HistoricalDataFetcher.DATETIME_FORMAT)} to '
        return_str = return_str + f'{self.end_datetime.strftime(HistoricalDataFetcher.DATETIME_FORMAT)} with symbols: '
        return_str = return_str + f'{str(self.feed_symbols)}'
        return return_str
    
    def add_feed(self, list_of_patterns, data_type: DataType, append=True):
        # if isinstance(list_of_patterns,list);
        (matched_symbols, df) = self.fetcher.fetch_from_pattern_list(list_of_patterns,
                                                                     self.start_datetime,
                                                                     self.end_datetime,
                                                                     add_symbol=True)
        # TODO: upload based on returned type
        uploaded_name = self.name + "_OrderBook_" + uuid.uuid4().hex if self.data_dictionary['OrderBook'] else \
                        self.data_dictionary['OrderBook'].keys()[0]
        
        DataChannel.upload(df, uploaded_name, is_overwrite=~append)
        self.feed_symbols.extend(matched_symbols)
        self.feed_symbols = list(set(self.feed_symbols))
        
        # TODO: implement based on returned type
        # necessary to download in case of append
        if append:
            self.data_dictionary['OrderBook'][uploaded_name] = DataChannel.download(uploaded_name)
        
        pass
    
    def add_subscriber(self, feed_subscriber: FeedSubscriber):
        
        assert self.data_dictionary['OrderBook']
        
        self.subscribers_dispatch[feed_subscriber.uuid] = [] # will set the indices to subscribe to for this subscriber
        pass

    def _get_subsribed_indices(self, filtration: Filtration):
        
        for local_filter in filtration.filters:
            local_filter
            
            
        
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
