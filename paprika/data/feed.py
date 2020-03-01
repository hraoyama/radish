from paprika.data.fetcher import HistoricalDataFetcher
from paprika.data.data_channel import DataChannel
from paprika.data.fetcher import DataType
from paprika.data.feed_filter import Filtration, TimeFreqFilter
from paprika.data.constants import TimePeriod

import os
import sys
from datetime import datetime
import uuid
import functools
import pandas as pd

sys.path.append(os.getenv("RADISH_PATH"))
sys.path.append(os.getenv("RADISH_DIR"))


class Feed:
    def __init__(self, name: str, start=datetime(1900, 1, 1), end=datetime(2200, 1, 1)):
        super().__init__()
        assert end > start
        self.start_datetime = start
        self.end_datetime = end
        self.name = name.upper().strip()
        self.feed_symbols = []
        self.fetcher = HistoricalDataFetcher()

        self.subscribers_dispatch = dict()

        # At this point we only support one DataFrame per Data Type
        # (Individual DataFrames can be aggregated from multiple feeds)
        self.data_dictionary = dict()

    def __str__(self):
        return_str = f'Feed {self.name} {self.start_datetime.strftime(HistoricalDataFetcher.DATETIME_FORMAT)} to '
        return_str = return_str + f'{self.end_datetime.strftime(HistoricalDataFetcher.DATETIME_FORMAT)} with symbols: '
        return_str = return_str + f'{str(self.feed_symbols)}'
        return return_str

    def set_feed(self, list_of_patterns, data_type, how='outer'):

        if isinstance(list_of_patterns, str):
            list_of_patterns = [list_of_patterns]

        # TODO too slow, replace by DataChannel
        # (matched_symbols, df) = self.fetcher.fetch_from_pattern_list(
        #     self.fetcher.generate_simple_pattern_list(list_of_patterns, data_type),
        #     self.start_datetime,
        #     self.end_datetime,
        #     add_symbol=True,
        #     shared_index=(how == 'inner'))

        df = DataChannel.fetch(list_of_patterns,
                               data_type=data_type,
                               start=self.start_datetime,
                               end=self.end_datetime)
        df.reset_index(inplace=True)
        df.set_index(DataChannel.DATA_INDEX, inplace=True)

        matched_symbols = df[DataChannel.SYMBOL_INDEX].unique().tolist()

        # if DataChannel.SYMBOL_INDEX in df.columns:
        #     df = df.drop([DataChannel.SYMBOL_INDEX], axis=1)

        uploaded_name = self.name + "_" + str(data_type) + "_" + uuid.uuid4().hex

        if df is None:
            raise ValueError(
                f"No data found matching {str(list_of_patterns)} for type {data_type}. Load data if available first.")

        if df.empty or df.shape[0] == 0:
            return None
            # no data for this time span but this name was found

        df = df.sort_index()  # this is important when combining sources in the same data frame

        # it is possible that we have multiple duplicated indices at the MS level...
        # do not remove them because in orderbook situations they can be valid
        # df = df.loc[~df.index.duplicated(keep='first')]

        DataChannel.upload_to_redis(df, uploaded_name)

        self.feed_symbols.extend(matched_symbols)
        self.feed_symbols = list(set(self.feed_symbols))
        # necessary to download in case of append
        self.data_dictionary[data_type] = df  # DataChannel.download(uploaded_name)

        pass

    def add_subscriber(self, feed_subscriber):

        from paprika.data.feed_subscriber import FeedSubscriber
        assert isinstance(feed_subscriber, FeedSubscriber)
        if feed_subscriber.subscribed_feed is not None:
            feed_subscriber.clear_feed()

        if not feed_subscriber.subscribed_feed:
            feed_subscriber.subscribed_feed = self

        subscribed = False
        if not feed_subscriber.filtrations:
            feed_subscriber.add_filtration(Filtration(TimeFreqFilter(TimePeriod.CONTINUOUS)))

        for filter_spec in feed_subscriber.filtrations:
            subscribed_indices = self._get_subscribed_indices(filter_spec)
            if subscribed_indices:
                subscribed = True
                for subscribed_index_set in subscribed_indices:
                    if subscribed_index_set[0] not in self.subscribers_dispatch.keys():
                        self.subscribers_dispatch[subscribed_index_set[0]] = {}
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

        # # TODO: this seems like a slow implementation...
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

        sorted_indices = sorted(list(set(sorted_indices)))

        to_dispatch_for_data_type = dict()
        for data_type in self.data_dictionary.keys():
            # https://stackoverflow.com/questions/49830069/efficiently-extract-rows-from-a-pandas-dataframe-ignoring-missing-index-labels
            # to_dispatch_for_data_type[data_type] = self.data_dictionary[data_type].loc[
            #     set(self.data_dictionary[data_type].index).intersection(sorted_indices)]
            indices_to_use = list(set(self.data_dictionary[data_type].index).intersection(sorted_indices))
            to_dispatch_for_data_type[data_type] = self.data_dictionary[data_type].loc[indices_to_use].sort_index()

        for index in sorted_indices:
            if index is None:
                continue
            # usually this is only 1 data type matching a specific time, seems like a lot of work for edge cases
            events = []
            passed_data_types = dispatched_indices[index]
            for data_type in passed_data_types:
                if index in to_dispatch_for_data_type[data_type].index:
                    # https://stackoverflow.com/questions/23521511/pandas-creating-dataframe-from-series
                    data_to_send = to_dispatch_for_data_type[data_type].loc[:index]
                    if len(data_to_send.shape) == 1:
                        events.append(tuple((data_type, pd.DataFrame([data_to_send],
                                                                     columns=data_to_send.index.values))))
                    else:
                        events.append(tuple((data_type, data_to_send)))
            if events:
                feed_subscriber.handle_event(events)

    @property
    def shape(self):
        return dict([(data_type, df.shape if df is not None else None) for data_type, df in
                     self.data_dictionary.items()])
