
from datetime import datetime
from pprint import pprint as pp

from paprika.data.fetcher import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.feed import Feed
from paprika.data.feed_filter import *
from paprika.signals.tryout_signal import TryOutSignal, TryOutCompositeSignal


def test_tryout_signals():
    # clear out any feeds in the cache
    DataChannel.clear_all_feeds()
    
    # set some paramaters you will use
    tryout_signal = TryOutSignal(BUY=100, SELL=10)
    pp(tryout_signal.parameters)
    
    # set some filters on the received events (the filters are a UNION of all filters)
    tryout_filtration = Filtration()
    tryout_filtration.add_filter(TimeFreqFilter(TimePeriod.HOUR, 5, indexing=TimeIndexing.BEFORE))  # default
    tryout_signal.add_filtration(tryout_filtration)
    
    # set up a source of two different data types that your signal will consume
    mixed_type_feed = Feed('DAX_FEED', datetime(2017, 5, 31), datetime(2017, 6, 4))
    mixed_type_feed.set_feed(["EUX.FDAX201709"], DataType.ORDERBOOK)
    mixed_type_feed.set_feed(["EUX.FDAX201709"], DataType.TRADES)
    
    # make your signal listen to the feed
    mixed_type_feed.add_subscriber(tryout_signal)
    
    # execute your signal
    tryout_signal.run()
    
    # show some of your results
    pp(tryout_signal.get_parameter("OBSERVATION"))
    pp(tryout_signal.get_parameter("COUNT"))
    
    # we can re-use the signal (think of what it is doing to any of your user parameters...)
    tryout_signal.clear_feed()
    tryout_signal.clear_filtration()
    tryout_signal.set_parameter("OBSERVATION", [])
    
    # use a different filter
    newSetOfFilters = Filtration()
    newSetOfFilters.add_filter(TimeFreqFilter(TimePeriod.BUSINESS_DAY, 1, starting=datetime(2017, 5, 31)))
    tryout_signal.add_filtration(newSetOfFilters)
    
    # set up a filter for trades only but of different inputs
    multiple_instrument_feed = Feed('MIXED_FEED', datetime(2017, 5, 15), datetime(2017, 6, 15))
    multiple_instrument_feed.set_feed(["EUX.FDAX201709", "MTA.IT0001250932"], DataType.TRADES)
    
    # make your signal listen to the feed
    multiple_instrument_feed.add_subscriber(tryout_signal)
    
    # execute the signal
    tryout_signal.run()
    
    # show some of your results
    pp(tryout_signal.get_parameter("OBSERVATION"))
    print(tryout_signal.get_parameter("COUNT"))
    
    # An example of composite signals, NUM_RND determines how many signals
    composite_signal = TryOutCompositeSignal(NUM_RND=5, P1=[45.2, 90.4], P2={"L1": 12, "SENSITIVITIES": [65.6, 49.9]})
    composite_signal.add_filtration(tryout_filtration)
    multiple_instrument_feed.add_subscriber(composite_signal)
    composite_signal.run()
    
    # what did this composite signal say?
    print(composite_signal.get_parameter("RANDOM_MEASURE"))
    
    # clear out any feeds in the cache
    DataChannel.clear_all_feeds()
