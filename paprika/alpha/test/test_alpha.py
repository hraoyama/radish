from paprika.alpha.alpha.alpha_01 import *
from paprika.alpha.base import Alpha
from paprika.data.data_processor import DataProcessor
from paprika.data.feed_filter import TimeFreqFilter, TimePeriod
from paprika.data.data_channel import DataChannel


def test_alpha():
    # symbols = DataChannel.check_register(["\w*.\w*.Trade"], False)
    # start = datetime(2010, 6, 1, 8, 15, 0)
    # end = datetime(2019, 6, 1, 8, 15, 0)
    # time_filter = TimeFreqFilter(TimePeriod.HOUR, 1)
    # dp = DataProcessor(symbols[0:2]).index(start, end)
    # dp.ohlcv(time_filter, inplace=True)

    symbols2 = DataChannel.check_register(['SP500.\w*.EOD'], False)
    time_filter2 = TimeFreqFilter(TimePeriod.WEEK, 1)
    dp2 = DataProcessor(symbols2[0:20])
    dp2.ohlcv(time_filter2, inplace=True)

    alpha = Alpha(dp2)
    print(alpha.list_alpha_universe())
    print(alpha.alpha_info('alpha_3'))
    alpha.calc_alpha(['alpha_\w*'])
    print(alpha.list_alpha())

    def alpha_5(dp, g):
        return dp.close * g

    alpha.add_alpha(alpha_5, 3)
    print(alpha.list_alpha())

    alpha.analyze(symbols2[0])
