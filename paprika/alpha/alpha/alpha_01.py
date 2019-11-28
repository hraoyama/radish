from paprika.alpha.base import *
from paprika.alpha.utils import *
from paprika.data.data_processor import DataProcessor


def alpha_3(dp: DataProcessor):
    return - correlation(rank(dp.open), rank(dp.volume), 10)


def alpha_4(dp: DataProcessor):
    return - ts_rank(rank(dp.low), 9)