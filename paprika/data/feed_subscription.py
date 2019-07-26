from paprika.data.fetcher import HistoricalDataFetcher
from paprika.data.fetcher import DataChannel
from paprika.data.fetcher import DataType
from paprika.data.feed_filter import Filtration

import os
import sys
from datetime import datetime
import logging
import uuid
import functools

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
        
        # At this point we only support one DataFrame per Data Type
        # (Individual DataFrames can be aggregated from multiple feeds)
        self.data_dictionary = dict()
        self.data_dictionary[DataType.ORDERBOOK] = None
        self.data_dictionary[DataType.TRADES] = None
    
    def __str__(self):
        return_str = f'Feed {self.name} {self.start_datetime.strftime(HistoricalDataFetcher.DATETIME_FORMAT)} to '
        return_str = return_str + f'{self.end_datetime.strftime(HistoricalDataFetcher.DATETIME_FORMAT)} with symbols: '
        return_str = return_str + f'{str(self.feed_symbols)}'
        return return_str
    
    def set_feed(self, list_of_patterns, data_type: DataType):
        
        assert data_type in self.data_dictionary.keys()
        
        (matched_symbols, df) = self.fetcher.fetch_from_pattern_list(
            self.fetcher.generate_simple_pattern_list(list_of_patterns, data_type),
            self.start_datetime,
            self.end_datetime,
            add_symbol=True)
        
        uploaded_name = self.name + "_" + str(data_type) + "_" + uuid.uuid4().hex
        
        df = df.sort_index() # this is important when combining sources in the same data frame
        
        # it is possible that we have multiple duplicated indices at the MS level...
        # do not remove them because in orderbook situations they can be valid
        # df = df.loc[~df.index.duplicated(keep='first')]
        
        DataChannel.upload(df, uploaded_name, is_overwrite=True)
        
        self.feed_symbols.extend(matched_symbols)
        self.feed_symbols = list(set(self.feed_symbols))
        # necessary to download in case of append
        self.data_dictionary[data_type] = DataChannel.download(uploaded_name)
        
        pass
    
    def add_subscriber(self, feed_subscriber):
        
        from paprika.data.feed_subscriber import FeedSubscriber
        assert isinstance(feed_subscriber, FeedSubscriber)
        if feed_subscriber.subscribed_feed is not None:
            feed_subscriber.clear_feed()
        
        if not feed_subscriber.subscribed_feed:
            feed_subscriber.subscribed_feed = self
        
        subscribed = False
        for filter_spec in feed_subscriber.filtrations:
            subscribed_indices = self._get_subscribed_indices(filter_spec)
            if subscribed_indices:
                subscribed = True
                for subscribed_index_set in subscribed_indices:
                    self.subscribers_dispatch[subscribed_index_set[0]][feed_subscriber.uuid] = [
                        subscribed_index_set[1]]
        if subscribed:
            feed_subscriber.subscribed_feed = self
    
    def remove_subscriber(self, feed_subscriber):
        from paprika.data.feed_subscriber import FeedSubscriber
        assert isinstance(feed_subscriber, FeedSubscriber)
        unsubscribed_data_types = []
        for data_type in self.subscribers_dispatch.keys():
            if feed_subscriber.uuid in self.subscribers_dispatch[data_type]:
                self.subscribers_dispatch.pop(feed_subscriber.uuid)
                unsubscribed_data_types.append(data_type)
        return unsubscribed_data_types
    
    def clear_subscribers(self):
        # Note that at this point subscribers will no longer be able to access the feed despite having callback access
        # if they want access to the feed, they will need to re-subscribe
        self.subscribers_dispatch = {}
    
    def _get_subscribed_indices(self, filtration: Filtration):
        assert isinstance(filtration, Filtration)
        # TODO: have a simple check for individual filter instead of filtration...
        subscribed_indices = []
        for data_type, data_indices in self.data_dictionary.items():
            if data_indices is not None:
                # data_indices is None when this feed does not supply this data type
                filtered_indices_from_multiple_filters = filtration.apply(data_indices)
                filtered_indices = sorted(list(functools.reduce(lambda x1, x2: set(x1).union(set(x2)),
                                                                filtered_indices_from_multiple_filters))) if len(
                    filtered_indices_from_multiple_filters) > 1 else filtered_indices_from_multiple_filters
                subscribed_indices.append((data_type, filtered_indices))
        
        return subscribed_indices
    
    def run(self, feed_subscriber):
        from paprika.data.feed_subscriber import FeedSubscriber
        assert isinstance(feed_subscriber, FeedSubscriber)
        
        # TODO: this seems like a slow implementation...
        dispatched_indices = dict()
        sorted_indices = []
        for data_type in self.subscribers_dispatch.keys():
            if feed_subscriber.uuid in self.subscribers_dispatch[data_type]:
                for dt_index_list_of_lists_per_data_type in self.subscribers_dispatch[data_type][feed_subscriber.uuid]:
                    for dt_index_list in dt_index_list_of_lists_per_data_type:
                        sorted_indices.extend(dt_index_list)
                        for dt_index in dt_index_list:
                            if dt_index in dispatched_indices:
                                dispatched_indices[dt_index].append(data_type)
                            else:
                                dispatched_indices[dt_index] = [data_type]
        
        sorted_indices = sorted(sorted_indices)
        
        for index in sorted_indices:
            data_types = dispatched_indices[index]
            data_frames = []
            passed_data_types =[]
            for data_type in data_types:
                if index in self.data_dictionary[data_type].index:
                    data_frames.append(self.data_dictionary[data_type].loc[index])
                    passed_data_types.append(data_type)
            feed_subscriber.handle_event(list(zip(passed_data_types, data_frames)))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])
    starting_time = datetime(2017, 5, 31)
    ending_time = datetime(2017, 6, 5)
    my_feed = FeedSubscription("my feed", starting_time, ending_time)
    my_feed.add_feed([".*MTA.IT0001250932.*"], DataType.ORDERBOOK)
    print(my_feed.feed_symbols)
    my_feed.add_feed([".*MTA.IT0001250932.*"], DataType.ORDERBOOK)
    print(my_feed.feed_symbols)
    my_feed.add_feed([".*EUX.FDAX201709.*"], DataType.ORDERBOOK)
    print(my_feed.feed_symbols)
    print(my_feed)
