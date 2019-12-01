from paprika.alpha.base import *
from paprika.alpha.utils import *
from paprika.data.data_processor import DataProcessor


def alpha_3(dp: DataProcessor, period=10):
    return - correlation(rank(dp.open), rank(dp.volume), period)


alpha_3.info = "- correlation(rank(dp.open), rank(dp.volume), period)"


def alpha_4(dp: DataProcessor, period=9):
    return - ts_rank(rank(dp.low), period)
