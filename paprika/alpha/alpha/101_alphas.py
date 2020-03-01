from paprika.alpha.base import *
from paprika.alpha.utils import *
from paprika.data.data_processor import DataProcessor


def alpha_1(dp: DataProcessor, period=20):
    # Alpha#1: (rank(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) - 0.5)
    """
    Doesn't make sense as vol compared against close price
    """
    returns_ = returns(dp.close, 1)
    signed_df = (returns_ < 0) * stddev(returns_, period) + (returns_ >= 0) * dp.close

    return rank(ts_argmax(np.power(signed_df, 2), 5) - 0.5)


def alpha_2(dp: DataProcessor, period=6):
    # Alpha#2: (-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
    r1 = rank(delta(dp.volume.apply(np.log), 2))
    r2 = rank((dp.close - dp.open) / dp.open)
    return -1 * correlation(r1, r2, period)


def alpha_3(dp: DataProcessor, period=10):
    return - correlation(rank(dp.open), rank(dp.volume), period)


alpha_3.info = "- correlation(rank(dp.open), rank(dp.volume), period)"


def alpha_4(dp: DataProcessor, period=9):
    return - ts_rank(rank(dp.low), period)


def alpha_5(dp: DataProcessor, period=10):
    # Alpha#5: rank(open - sum(vwap, 10) / 10) * -1 * abs(rank(close - vwap))
    """
    Don't understand why abs is needed.
    """
    return -1 * rank(dp.open - ts_mean(dp.vwap, period)) * rank(dp.close - dp.vwap)


def alpha_6(dp: DataProcessor):
    # Alpha#6: (-1 * correlation(open, volume, 10))
    return -1 * correlation(dp.open, dp.volume, 10)


def alpha_7(dp: DataProcessor, period=7, my_rank=60):
    # Alpha#7: (adv20 < volume ? -1 * ts_rank(abs(delta(close, 7)), 60) * sign(delta(close, 7)) : (-1* 1))
    """
    Don't understand adv20 (in $) < volume (in units of shares). Weird else condition
    """
    adv20 = ts_mean(dp.volume * dp.close, 20)
    cond = adv20 < (dp.volume * dp.close)

    return cond * (-1 * ts_rank(np.abs(delta(dp.close, period)), my_rank) * np.sign(delta(dp.close, period))) + (
        ~cond) * -1
