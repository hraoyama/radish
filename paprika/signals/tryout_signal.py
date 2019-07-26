from typing import List, Tuple
from datetime import datetime

from paprika.data.fetcher import DataType
from paprika.data.fetcher import DataChannel
from paprika.data.feed_subscription import FeedSubscription
from paprika.data.feed_subscriber import FeedSubscriber
from paprika.data.feed_filter import *


class TryOutSignal(FeedSubscriber):
    def __init__(self, **kwargs):
        super(TryOutSignal, self).__init__(**kwargs)
    
    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):
        super(TryOutSignal, self).handle_event(events)
        self._parameter_dict["COUNT"] = self.call_count
        for event in events:
            print(str(event[0]))
            print(event[1])


if __name__ == "__main__":
    # clear out any feeds in the cache
    DataChannel.clear_all_feeds()
    
    # set some paramaters you will use
    tryOut = TryOutSignal(BUY=100, SELL=10)
    print(tryOut.parameters)
    
    # set some filters on the received events (the filters are a UNION of all filters)
    tryOutFiltration = Filtration()
    tryOutFiltration.add_filter(TimeFreqFilter(TimePeriod.HOUR, 5))
    tryOut.add_filtration(tryOutFiltration)

    # set up a source of two different data types that your signal will consume
    mixed_type_feed = FeedSubscription('DAX_FEED', datetime(2017, 5, 31), datetime(2017, 6, 4))
    mixed_type_feed.set_feed(["EUX.FDAX201709"], DataType.ORDERBOOK)
    mixed_type_feed.set_feed(["EUX.FDAX201709"], DataType.TRADES)

    # make your signal listen to the feed
    mixed_type_feed.add_subscriber(tryOut)

    # execute your signal
    tryOut.run()

    # we can re-use the signal (think of what it is doing to any of your user parameters...)
    tryOut.clear_feed()
    tryOut.clear_filtration()
    
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
    print(tryOut.get_parameter("COUNT"))
    
    # clear out any feeds in the cache
    DataChannel.clear_all_feeds()
