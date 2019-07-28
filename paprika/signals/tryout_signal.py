import numpy as np
from typing import List, Tuple
from datetime import datetime

from paprika.data.fetcher import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.feed_subscription import FeedSubscription
from paprika.data.feed_subscriber import FeedSubscriber
from paprika.data.feed_filter import *


class TryOutSignal(FeedSubscriber):
    def __init__(self, **kwargs):
        super(TryOutSignal, self).__init__(**kwargs)
        self._parameter_dict["OBSERVATION"] = []
    
    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):
        super(TryOutSignal, self).handle_event(events)
        self._parameter_dict["COUNT"] = self.call_count
        for event in events:
            self._parameter_dict["OBSERVATION"].append(
                (event[0], event[1].loc[['ISIN']]))
            # 'Date', 'MarketTime', 'TimeSec', 'TimeMM'


class RandomSignal(FeedSubscriber):
    def __init__(self, **kwargs):
        super(RandomSignal, self).__init__(**kwargs)
    
    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):
        return np.random.randn(1)[0]


class TryOutCompositeSignal(FeedSubscriber):
    def __init__(self, **kwargs):
        super(TryOutCompositeSignal, self).__init__(**kwargs)
        self._MEASURE = "RANDOM_MEASURE"
        self._parameter_dict[self._MEASURE] = []
        self.member_signals = []
        if "NUM_RND" in self._parameter_dict.keys():
            for i in range(int(self.get_parameter("NUM_RND"))):
                self.member_signals.append(RandomSignal())
    
    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):
        super(TryOutCompositeSignal, self).handle_event(events)
        self._parameter_dict["COUNT"] = self.call_count
        self.get_parameter(self._MEASURE).append(sum([x.handle_event(events) for x in self.member_signals]))
        # events[0][1]


if __name__ == "__main__":
    # clear out any feeds in the cache
    DataChannel.clear_all_feeds()
    
    # set some paramaters you will use
    tryOut = TryOutSignal(BUY=100, SELL=10)
    print(tryOut.parameters)
    
    # set some filters on the received events (the filters are a UNION of all filters)
    tryOutFiltration = Filtration()
    tryOutFiltration.add_filter(TimeFreqFilter(TimePeriod.HOUR, 5, indexing=TimeIndexing.BEFORE))  # default
    tryOut.add_filtration(tryOutFiltration)
    
    # set up a source of two different data types that your signal will consume
    mixed_type_feed = FeedSubscription('DAX_FEED', datetime(2017, 5, 31), datetime(2017, 6, 4))
    mixed_type_feed.set_feed(["EUX.FDAX201709"], DataType.ORDERBOOK)
    mixed_type_feed.set_feed(["EUX.FDAX201709"], DataType.TRADES)
    
    # make your signal listen to the feed
    mixed_type_feed.add_subscriber(tryOut)
    
    # execute your signal
    tryOut.run()
    
    # show some of your results
    print(tryOut.get_parameter("OBSERVATION"))
    print(tryOut.get_parameter("COUNT"))
    
    # we can re-use the signal (think of what it is doing to any of your user parameters...)
    tryOut.clear_feed()
    tryOut.clear_filtration()
    tryOut.set_parameter("OBSERVATION", [])
    
    # use a different filter
    newSetOfFilters = Filtration()
    newSetOfFilters.add_filter(TimeFreqFilter(TimePeriod.BUSINESS_DAY, 1, starting=datetime(2017, 5, 31)))
    tryOut.add_filtration(newSetOfFilters)
    
    # set up a filter for trades only but of different inputs
    multiple_instrument_feed = FeedSubscription('MIXED_FEED', datetime(2017, 5, 15), datetime(2017, 6, 15))
    multiple_instrument_feed.set_feed(["EUX.FDAX201709", "MTA.IT0001250932"], DataType.TRADES)
    
    # make your signal listen to the feed
    multiple_instrument_feed.add_subscriber(tryOut)
    
    # execute the signal
    tryOut.run()
    
    # show some of your results
    print(tryOut.get_parameter("OBSERVATION"))
    print(tryOut.get_parameter("COUNT"))
    
    # An example of composite signals, NUM_RND determines how many signals
    compositeSignal = TryOutCompositeSignal(NUM_RND=5, P1=[45.2, 90.4], P2={"L1": 12, "SENSITIVITIES": [65.6, 49.9]})
    compositeSignal.add_filtration(tryOutFiltration)
    multiple_instrument_feed.add_subscriber(compositeSignal)
    compositeSignal.run()

    # what did this composite signal say?
    print(compositeSignal.get_parameter("RANDOM_MEASURE"))
    
    # clear out any feeds in the cache
    DataChannel.clear_all_feeds()
