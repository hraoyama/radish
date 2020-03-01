from paprika.alpha.base import *
from paprika.alpha.utils import *
from paprika.data.data_processor import DataProcessor
import numpy as np


def alpha_1(dp: DataProcessor, period=20):
    """
    rank(ts_argmax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) - 0.5
    """
    # Doesn't make sense as vol compared against close price
    returns_ = returns(dp.close, 1)
    signed_df = (returns_ < 0) * stddev(returns_, period) + (returns_ >= 0) * dp.close
    return rank(ts_argmax(np.power(signed_df, 2), 5) - 0.5)


def alpha_2(dp: DataProcessor, period=6):
    """
    - correlation(rank(delta(log(volume), 2)), rank((close - open) / open, 6))
    """
    r1 = rank(delta(dp.volume.apply(np.log), 2))
    r2 = rank((dp.close - dp.open) / dp.open)
    return - correlation(r1, r2, period)


def alpha_3(dp: DataProcessor, period=10):
    """
    - correlation(rank(dp.open), rank(dp.volume), period)
    """
    return - correlation(rank(dp.open), rank(dp.volume), period)


alpha_3.info = "- correlation(rank(dp.open), rank(dp.volume), period)"


def alpha_4(dp: DataProcessor, period=9):
    """
    - ts_rank(rank(low), 9)
    """
    return - ts_rank(rank(dp.low), period)


def alpha_5(dp: DataProcessor, period=10):
    """
    rank(open - sum(vwap, 10) / 10) * -1 * abs(rank(close - vwap))
    """
    # Don't understand why abs is needed.
    return - rank(dp.open - ts_mean(dp.vwap, period)) * rank(dp.close - dp.vwap)


def alpha_6(dp: DataProcessor, period=10):
    """
    - correlation(open, volume, 10)
    """
    return - correlation(dp.open, dp.volume, period)


def alpha_7(dp: DataProcessor, period=7, my_rank=60):
    """
    adv20 < volume ? -1 * ts_rank(abs(delta(close, 7)), 60) * sign(delta(close, 7)) : -1
    """

    # Don't understand adv20 (in $) < volume (in units of shares). Weird else condition
    adv20 = ts_mean(dp.volume * dp.close, 20)
    cond = adv20 < (dp.volume * dp.close)

    return cond * (-1 * ts_rank(np.abs(delta(dp.close, period)), my_rank) * np.sign(delta(dp.close, period))) + (
        ~cond) * -1


def alpha_8(dp: DataProcessor, period=5, delay_period=10):
    """
    -1 * rank((sum(open, 5) * sum(returns, 5)) - delay((sum(open, 5) * sum(returns, 5)), 10))
    """
    returns_ = returns(dp.close, 1)
    r1 = ts_sum(dp.open, period) * ts_sum(returns_, period)
    return - rank(r1 - delay(r1, delay_period))


def alpha_9(dp: DataProcessor, period=5):
    """
    0 < ts_min(delta(close, 1), 5) ? delta(close, 1) :
            ((ts_max(delta(close, 1), 5) < 0) ? delta(close, 1) : (-1 * delta(close, 1)))
    """
    diff = delta(dp.close, 1)
    cond = ts_min(diff, period)
    cond2 = ts_max(diff, period)
    return (cond > 0) * diff + (cond <= 0) * ((cond2 < 0) - (cond2 >= 0)) * diff


def alpha_10():
    """
    rank(((0 < ts_min(delta(close, 1), 4)) ? delta(close, 1) :
             ((ts_max(delta(close, 1), 4) < 0) ? delta(close, 1) : (-1 * delta(close, 1)))))
    """
    # Don't understand condition on rank (it is not boolean). Also looks as alpha_9
    raise NotImplementedError("Description does not seem valid and resembles to alpha_9")


def alpha_11(dp: DataProcessor, period=3):
    """
    (rank(ts_max((vwap - close), 3)) + rank(ts_min((vwap - close), 3))) * rank(delta(volume, 3))
    """
    diff = dp.vwap - dp.close
    return (rank(ts_max(diff, period)) + rank(ts_min(diff, period))) * rank(delta(dp.volume, period))


def alpha_12(dp: DataProcessor):
    """
    sign(delta(volume, 1)) * (-1 * delta(close, 1))
    """
    return - np.sign(delta(dp.volume, 1)) * delta(dp.close, 1)


def alpha_13(dp: DataProcessor):
    """
    -1 * rank(covariance(rank(close), rank(volume), 5))
    """
    return - rank(covariance(rank(dp.close), rank(dp.volume), 5))


def alpha_14(dp: DataProcessor, period=10):
    """
    - rank(delta(returns, 3)) * correlation(open, volume, 10)
    """

    return - rank(delta(returns(dp.close, 1), 3)) * correlation(dp.open, dp.volume, period)


def alpha_15(dp: DataProcessor, period=3):
    """
    - sum(rank(correlation(rank(high), rank(volume), 3)), 3)
    """
    # Correlation computed on 3 points...
    return - ts_sum(rank(correlation(rank(dp.high), rank(dp.volume), period)), period)


def alpha_16(dp: DataProcessor, period=5):
    """
    - rank(covariance(rank(high), rank(volume), 5))
    """
    return - rank(covariance(rank(dp.high), rank(dp.volume), period))


def alpha_17(dp: DataProcessor):
    """
    - rank(ts_rank(close, 10)) * rank(delta(delta(close, 1), 1)) * rank(ts_rank((volume / adv20), 5))
    """
    # Volume (units are in shares) divided by $ volume. Corrected it.
    adv20 = ts_mean(dp.volume * dp.close, 20)
    return - rank(ts_rank(dp.close, 10)) * rank(delta(delta(dp.close, 1), 1)) * rank(ts_rank(dp.volume * dp.close / adv20, 5))


def alpha_18(dp: DataProcessor):
    """
    - rank(stddev(abs(close - open), 5) + (close - open) + correlation(close, open, 10))
    """
    # Doesn't make sense - vol is added with price changes and rolling correlation
    diff = dp.close - dp.open
    return - rank(stddev(np.abs(diff), 5) + diff + correlation(dp.close, dp.open, 10))


def alpha_19(dp: DataProcessor):
    """
    - sign(delta(close, 7)) * (1 + rank(1 + sum(returns, 250)))
    """
    return - np.sign(delta(dp.close, 7)) * (1 + rank(1 + ts_sum(returns(dp.close, 1), 250)))


def alpha_20(dp: DataProcessor):
    """
    - rank(open - delay(high, 1)) * rank(open - delay(close, 1)) * rank(open - delay(low, 1))
    """
    return - rank(dp.open - delay(dp.high, 1)) * rank(dp.open - delay(dp.close, 1)) * rank(dp.open - delay(dp.low, 1))


