from functools import partial
from datetime import datetime
from multiprocessing import Pool

from paprika.alpha.base import *
from paprika.data.data_processor import DataProcessor
from paprika.data.feed_filter import TimeFreqFilter, TimePeriod
from paprika.data.data_channel import DataChannel
from paprika.data.fetcher import HistoricalDataFetcher
from paprika.utils.utils import first, last


def test_base():
    symbols = DataChannel.check_register(['EUX.*Trade'], False)
    start = '2017-06-01 08:00'
    end = '2017-06-3 08:00'
    time_filter = TimeFreqFilter(TimePeriod.MINUTE, 15, starting=datetime(2017, 6, 1, 8, 15, 0))

    dp = get_alpha_data(symbols[0:3], time_filter, start, end)
    print(dp.close.head)
