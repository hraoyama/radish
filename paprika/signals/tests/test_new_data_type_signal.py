import numpy as np
from datetime import datetime, timedelta
from pprint import pprint as pp

from paprika.data.data_type import DataType
from paprika.utils.record import Timeseries
from paprika.data.data_channel import DataChannel
from paprika.data.feed import Feed
from paprika.data.feed_filter import Filtration, TimeFreqFilter, TimePeriod
from paprika.signals.tryout_signal import TryOutSignal


def test_new_data_type_signal():
    
    # create some new data with a new data type
    start = datetime.now()
    end = start + timedelta(days=20)
    random_dates = start + (end - start) * np.random.random([1,100])
    random_dates.sort()
    random_numbers = np.random.randn(1000)
    new_data = Timeseries()
    for ts, val in zip(list(random_dates[0]),random_numbers):
        new_data.append(ts, val)
    DataType.extend('TIMESERIES')

    # puts this data to make it available for as a Feed
    table_name = DataChannel.name_to_data_type("MyRandomIdentifier", DataType.TIMESERIES)
    DataChannel.upload(new_data.to_dataframe(), table_name)
    # following stores it in DB permanently - only do this if you are sure you need to keep this data
    # DataChannel.upload_to_permanent(table_name)

    # set up a feed that will supply this data
    new_feed = Feed('My_New_Data_Feed', start + timedelta(days=1), end - timedelta(days=2))
    new_feed.set_feed(["MyRandomIdentifier"], DataType.TIMESERIES)

    # create a signal to use this feed
    tryout_signal = TryOutSignal(RANDOM_PARAMETER=100)
    tryout_signal.add_filtration(Filtration(TimeFreqFilter(TimePeriod.HOUR, 4)))

    # make your signal listen to the feed
    new_feed.add_subscriber(tryout_signal)

    # run your signal
    tryout_signal.run()

    # show some of your results
    pp(tryout_signal.get_parameter("OBSERVATION"))
    pp(tryout_signal.get_parameter("COUNT"))
    
    # clear out your feeds
    DataChannel.clear_all_feeds()

    pass


# if __name__ == "__main__":
#     test_new_data_type_signal()