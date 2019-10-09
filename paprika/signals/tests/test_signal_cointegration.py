from datetime import datetime
from pprint import pprint as pp

from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.feed import Feed
from paprika.data.feed_filter import TimeFreqFilter, Filtration, TimePeriod
from paprika.signals.signal_cointegration import CointegrationSpread


def test_cointegration_signal():
    # to check what is available:
    # print(DataChannel.table_names(arctic_source_name='mdb'))

    # set up the pairs data you will be looking at
    feed = Feed('CG', datetime(2018, 1, 30), datetime(2019, 3, 30))
    feed.set_feed(["ETF.FR0007054358", "ETF.DE0005933923"], DataType.TRADES)

    # how much data did we actually get?
    pp(feed.shape)

    signal = CointegrationSpread(BETA=1.631, MEAN=0.052196, STD=1.9487, ENTRY=1, EXIT=0.55)
    signal.add_filtration(Filtration(TimeFreqFilter(TimePeriod.SECOND, 3600)))
    feed.add_subscriber(signal)

    # execute the signal
    signal.run()

    # show some of your results
    print(signal.positions.head())

