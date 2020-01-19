
from datetime import datetime
from pprint import pprint as pp

from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.feed import Feed
from paprika.data.feed_filter import TimeFreqFilter, Filtration
from paprika.data.constants import TimePeriod
from paprika.signals.pair_spread_ols_signal import PairSpread

def test_pair_spread_signal():
    # to check what is available:
    # print(DataChannel.table_names(arctic_source_name='mdb'))
    
    # set up the pairs data you will be looking at
    pairs_feed = Feed('PAIRS_FEED', datetime(2017, 7, 1), datetime(2017, 7, 28))
    pairs_feed.set_feed(["EUX.FBTP201709", "EUX.FBTS201709"], DataType.TRADES)
    
    # how much data did we actually get?
    pp(pairs_feed.shape)
    
    pair_signal = PairSpread(Y="FBTP201709", X="FBTS201709", BETA=1.2, SD_SUPPLIED=1.2, SD_FACTOR=0.5)
    pair_signal.add_filtration(Filtration(TimeFreqFilter(TimePeriod.SECOND, 600)))
    pairs_feed.add_subscriber(pair_signal)
    
    # execute the signal
    pair_signal.run()
    
    # show some of your results
    print(pair_signal.get_parameter("WEIGHT_ALLOCATION"))
    print(pair_signal.get_parameter("BETA_HISTORY"))
    print(pair_signal.parameters)
    
    # let's adapt the beta with time
    pair_signal_2 = PairSpread(Y="FBTP201709", X="FBTS201709",
                               BETA=1.2, BETA_UPDATE_FACTOR=1.01,
                               SD_SUPPLIED=1.2, SD_FACTOR=0.5)
    pair_signal_2.add_filtration(Filtration(TimeFreqFilter(TimePeriod.SECOND, 600)))
    pairs_feed.add_subscriber(pair_signal_2)
    pair_signal_2.run()
    print(pair_signal_2.get_parameter("WEIGHT_ALLOCATION"))
    print(pair_signal_2.get_parameter("BETA_HISTORY"))
    print(pair_signal_2.parameters)
    
    # let's use the rolling historical standard deviation
    pair_signal_3 = PairSpread(Y="FBTP201709", X="FBTS201709",
                               BETA=1.2, SD_FACTOR=2.0, NUM_OBS=20.0)
    pair_signal_3.add_filtration(Filtration(TimeFreqFilter(TimePeriod.SECOND, 600)))
    pairs_feed.add_subscriber(pair_signal_3)
    pair_signal_3.run()
    print(pair_signal_3.get_parameter("WEIGHT_ALLOCATION"))
    print(pair_signal_3.get_parameter("BETA_HISTORY"))
    print(pair_signal_3.parameters)
    
    # let's use the rolling historical standard deviation and some adaptive beta
    pair_signal_4 = PairSpread(Y="FBTP201709", X="FBTS201709",
                               BETA=1.2, SD_FACTOR=1.0, NUM_OBS=20.0, BETA_UPDATE_FACTOR=1.05)
    pair_signal_4.add_filtration(Filtration(TimeFreqFilter(TimePeriod.SECOND, 600)))
    pairs_feed.add_subscriber(pair_signal_4)
    pair_signal_4.run()
    print(pair_signal_4.get_parameter("WEIGHT_ALLOCATION"))
    print(pair_signal_4.get_parameter("BETA_HISTORY"))
    print(pair_signal_4.parameters)
    
    # check that we don't get a speed penalty for accidentally creating a duplicated feed
    # set up the pairs data you will be looking at
    pairs_feed_duplicate = Feed('PAIRS_FEED_2', datetime(2017, 7, 1), datetime(2017, 7, 28))
    pairs_feed_duplicate.set_feed(["EUX.FBTP201709", "EUX.FBTS201709"], DataType.TRADES)
    
    # how much data did we actually get?
    pp(pairs_feed_duplicate.shape)
    
    pair_signal_5 = PairSpread(Y="FBTP201709", X="FBTS201709",
                               BETA=1.2, SD_FACTOR=1.0, NUM_OBS=40.0)
    pair_signal_5.add_filtration(Filtration(TimeFreqFilter(TimePeriod.SECOND, 600)))
    pairs_feed_duplicate.add_subscriber(pair_signal_5)
    
    # execute the signal
    pair_signal_5.run()
    
    # clear out your used feeds
    DataChannel.clear_all_feeds()
