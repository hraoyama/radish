from paprika.alpha.alpha.alpha_01 import *
from paprika.alpha.base import Alpha
from paprika.data.data_processor import DataProcessor
from paprika.data.feed_filter import TimeFreqFilter, TimePeriod
from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType


def test_alpha():

    symbols = DataChannel.check_register(['.*.Candle'])
    data = DataChannel.fetch(['SP500.A.*'], data_type=DataType.CANDLE)
    time_filter = TimeFreqFilter(TimePeriod.WEEK, 1)
    dp = DataProcessor(data)
    dp.ohlcv(time_filter, inplace=True)

    alpha = Alpha(dp)
    print(alpha.list_alpha_universe())
    print(alpha.alpha_info('alpha_3'))
    alpha.calc_alpha(['alpha_.*'])
    print(alpha['alpha_3'])
    print(alpha.list_alpha())

    def alpha_5(dp, g):
        return dp.close * g

    alpha.add_alpha(alpha_5, 3)
    print(alpha.list_alpha())

    corr_matrix = alpha.corr_between_alphas()
    return_alpha_corr = alpha.corr_between_alpha_and_return()
    # alpha.analyze_whole(predict_period=1)
    print('ok')
