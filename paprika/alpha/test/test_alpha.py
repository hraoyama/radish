from paprika.alpha.alpha.alpha_01 import *
from paprika.alpha.base import Alpha
from paprika.data.data_processor import DataProcessor
from paprika.data.feed_filter import TimeFreqFilter, TimePeriod
from paprika.data.data_channel import DataChannel


def test_alpha():
    symbols = DataChannel.check_register(['MTA.*Trade'], False)
    start = datetime(2017, 6, 1, 8, 15, 0)
    end = datetime(2019, 6, 1, 8, 15, 0)
    time_filter = TimeFreqFilter(TimePeriod.MINUTE, 15, starting=datetime(2017, 6, 1, 8, 15, 0))

    dp = DataProcessor(symbols[0:10], 'ohlcv', time_filter).index(start, end)

    alpha = Alpha(['alpha_\w*'], dp)
    print(alpha.list_alpha())
    print(alpha.list_alpha_universe())

    def alpha_5(dp):
        return dp.close

    alpha.add_alpha(alpha_5)
    print(alpha.list_alpha())

