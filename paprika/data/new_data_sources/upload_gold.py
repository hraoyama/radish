import pandas as pd
import os

from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.feed import Feed
from paprika.data.feed_filter import TimeFreqFilter, Filtration, TimePeriod
from paprika.signals.gold_cointegration import GoldSpread

PATH = r'../../../resources/data/'


def main():
    tckr1 = 'GLD'
    tckr2 = 'GDX'
    ts1 = pd.read_excel(os.path.join(PATH, '{}.xls'.format(tckr1)), usecols=[0, 6])
    ts1 = ts1.sort_values(by=['Date'])
    ts1 = ts1.reset_index(drop=True)

    ts2 = pd.read_excel(os.path.join(PATH, '{}.xls'.format(tckr2)), usecols=[0, 6])
    ts2 = ts2.sort_values(by=['Date'])
    ts2 = ts2.reset_index(drop=True)

    ts = pd.merge(ts2, ts1, how='inner', on=['Date'])
    ts.columns = ['date', 'GLD', 'GDX']
    ts.set_index('date', inplace=True)
    
    DataType.extend("EOD_PRICE")
    
    table_name = DataChannel.name_to_data_type("GOLD_GDX", DataType.EOD_PRICE)
    DataChannel.upload(ts, table_name)
    # following stores it in DB permanently - only do this if you are sure you need to keep this data
    # DataChannel.upload_to_permanent(table_name)

    gold_feed = Feed('GOLD_FEED', datetime(1950, 7, 1), datetime(2050, 1, 1))
    gold_feed.set_feed("GOLD_GDX", DataType.EOD_PRICE)

    gold_signal = GoldSpread(BETA=1.631, MEAN=0.052196, STD=1.9487, ENTRY=1, EXIT=0.5)
    gold_feed.add_subscriber(gold_signal)

    gold_signal.run()
    print(gold_signal.positions)


if __name__ == "__main__":
    main()
