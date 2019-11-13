from datetime import datetime
from math import ceil, floor
from typing import Any, Type

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import coint, adfuller
from sklearn.linear_model import LinearRegression
from scipy.stats import pearsonr


from paprika.utils.time import datetime_to_millis, millis_for_frequency, seconds_for_frequency
from paprika.utils.types import float_type

EPS = 1e-8


# these functions are here just so they have a name...
def first(x):
    return x[0]


def last(x):
    return x[-1]


# https://stackoverflow.com/questions/30399534/shift-elements-in-a-numpy-array
# preallocate empty array and assign slice by chrisaycock
def fast_shift(arr, num, fill_value=np.nan):
    result = np.empty_like(arr)
    if num > 0:
        result[:num] = fill_value
        result[num:] = arr[:-num]
    elif num < 0:
        result[num:] = fill_value
        result[:num] = arr[-num:]
    else:
        result[:] = arr
    return result


def apply_func(data, func, *args, **kwargs):
    member_func = getattr(data, str(func), None) if not isinstance(data, list) else None
    return member_func(*args, **kwargs) if member_func is not None else func(data, *args, **kwargs)


def summarize(data, funcs, **kwargs):
    column_names = None
    if kwargs:
        if 'column_names' in kwargs.keys():
            column_names = [str(x) for x in kwargs['column_names']]
    if column_names is None:
        column_names = [x.__name__ for x in funcs]
    if not column_names or not funcs:
        raise ValueError(
            f'Number of functions ({len(funcs)}) must be >0 and match the number of function names {len(column_names)}')
    
    if len(data) < 1:
        summary_dict = dict([(key, [np.nan]) for key, func in zip(column_names, funcs)])
    else:
        # the functions are independent from each other so do not need to be executed in a loop!
        summary_dict = dict([(key, [apply_func(data, func)]) for key, func in zip(column_names, funcs)])
    
    return pd.DataFrame.from_dict(summary_dict)


def isclose(value1, value2):
    t = float_type()
    return abs(t(value1) - t(value2)) < t(EPS)


def try_parse(to_type: Type, s: Any) -> Any:
    try:
        return to_type(s)
    except BaseException:
        return None


def parse_currency_pair(symbol):
    return symbol.split('/')


def currency_pair(base, quote):
    return f'{base}/{quote}'


def percentage(value, digits=3) -> str:
    return "{0:.{digits}f}%".format(value * 100, digits=digits)


# [start, end] TODO: use [start, end) everywhere.
def forward_fill_ohlcv(df, end=None, frequency=None):
    # TODO: Replace with Pandas resample:
    # https://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.resample.html
    
    if isinstance(df.index, pd.DatetimeIndex):
        timestamps = (df.index.astype(np.int64) // 10 ** 6).to_series()
    else:
        timestamps = df.index.to_series()
    
    start = timestamps.iloc[0]
    
    if end is None:
        end = timestamps.iloc[-1]
    if isinstance(end, datetime):
        end = datetime_to_millis(end)
    
    if frequency is None:
        step = timestamps.diff().value_counts().index[0]
    else:
        step = millis_for_frequency(frequency)
    
    start = ceil(start / step) * step
    end = floor(end / step) * step
    
    filled_index_ms = np.arange(start, end + 1, step, dtype=np.int64)
    filled_index_dt = pd.to_datetime(filled_index_ms, unit='ms')
    
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reindex(filled_index_dt)
    else:
        df = df.reindex(filled_index_ms)
    
    df['close'].fillna(method='ffill', inplace=True)
    df['open'].fillna(df['close'], inplace=True)
    df['high'].fillna(df['close'], inplace=True)
    df['low'].fillna(df['close'], inplace=True)
    df['volume'].fillna(0, inplace=True)
    
    return df


def forward_fill_to_ohlcv(df, end=None, frequency=None):
    df = df.loc[:, ['Price', 'Volume']]
    if end is None:
        end = df.index[-1]
    if end > df.index[-1]:
        end_df = pd.DataFrame(np.nan, index=[end], columns=df.columns)
        df = df.append(end_df)
    
    sec = seconds_for_frequency(frequency)
    tmp = df['Price'].resample(f'{sec}S').ohlc()
    
    tmp['close'].fillna(method='ffill', inplace=True)
    tmp['open'].fillna(tmp['close'], inplace=True)
    tmp['high'].fillna(tmp['close'], inplace=True)
    tmp['low'].fillna(tmp['close'], inplace=True)
    tmp['volume'] = df['Volume'].resample(f'{sec}S').sum()
    
    return tmp


def sharpe(excess_returns, period):
    """
    Computing Sharpe ratio. For the standard deviation, (N-1) scaling is used
    :param excess_returns: excess returns of the strategy
    :param period: period over which Sharpe ratio is computed
    :return: returns Sharpe ratio
    """
    return np.nanmean(excess_returns) * np.sqrt(period) / np.nanstd(excess_returns, ddof=1)


def array_shift(arr, shift):
    """
    Ugly implementation of array shift through pandas
    :param arr: array to shift
    :param shift: shift value
    :return: shifted array
    """
    return pd.DataFrame(arr).shift(shift).values


def drawdown_calculator(excess_returns):
    """
    Computation of maximum drawdown and maximum drawdown duration
    :param excess_returns: excess returns of the portfolio
    :return: tuple of max drawdown and duration
    """

    df = pd.DataFrame(excess_returns, columns=['net'])
    df['cum_ret'] = (1 + df['net']).cumprod() - 1
    df['high_mark'] = np.maximum.accumulate(df['cum_ret'].fillna(0))
    df.loc[0, 'high_mark'] = np.nan
    df['drawdown'] = (1 + df['cum_ret']) / (1 + df['high_mark']) - 1
    max_drawdown = np.min(df['drawdown'])

    for i in range(1, len(df)):
        df.loc[i, 'duration'] = 0 if df.loc[i, 'drawdown'] == 0 else 1 + df.loc[i-1, 'duration']
    max_drawdown_duration = np.max(df['duration'])

    return max_drawdown, max_drawdown_duration


def returns_calculator(prices, lag):
    """
    Non-overlapping returns at the given lag
    :param prices: dataframe or Series with prices
    :param lag: given lag
    :return: returns
    """
    return (prices / prices.shift(lag) - 1)[::lag]


def portfolio_return_calculator(positions, returns):
    """
    Computing portfolio's return from positions and returns
    :param positions: positions at each day
    :param returns: returns between t-1 and t
    :return: portfolio's return
    """
    shifted = array_shift(positions, 1)
    if len(positions.shape) == 1:
        return shifted.flatten() * returns
    else:
        return np.nansum(shifted * returns, axis=1)


def simple_transaction_costs(positions, cost):
    """
    Implementation of simple transaction model
    :param positions: positions at each day
    :param cost: cost per unit weight
    :return: total transaction costs
    """
    return np.nansum(np.abs(positions - array_shift(positions, 1)), axis=1) * cost

def cadf_test(y, x, **kwargs):
    """
    Performs Engle-Granger cointegration test
    :param y: y series
    :param x: x series
    :param kwargs: additional parameters for CADF test
    :return: test statistic, p-values and critical values
    """
    result = coint(y, x, **kwargs)
    return result


def johansen_test(dat, **kwargs):
    """
    Performs Johansen cointegration test.
    :param dat: input data (array like)
    :param kwargs: additional parameters for Johansen test (i.e., trend presence, lags)
    :return: fitted object.
    Attributes:
        lr1 and lr2 - trace and maximum eigen value statistics
        cvt and cvm - the respective critical values
        eig and evec - eigenvalues and eigenvectors
    """
    obj = vm.coint_johansen(dat, **kwargs)
    return obj


def correlation_test(x1, x2):
    """
    Pearson's correlation test of significance
    :param x1: first series
    :param x2: second series
    :return: correlation and its p_value
    """
    x = pd.DataFrame([x1, x2]).T.dropna().values
    return pearsonr(x[:, 0], x[:, 1])


def half_life(z):
    """
    Computing half-life of mean-reverting strategy based
    on the Ornstein-Uhlenbeck approximation of the process

    :param z: input time series represented by pandas.Series (i.e., z-score)
    :return: half-life
    """
    dz = (z - z.shift(1))[1:]
    prev_z = z.shift(1)[1:].values.reshape(-1, 1)
    fit_obj = LinearRegression().fit(prev_z - np.mean(z), dz)

    return - np.log(2) / fit_obj.coef_[0]


def adf_test(z, **kwargs):
    """
    Performs Augmented-Dickey Fuller Test
    :param z: input time series
    :param kwargs: additional parameters for augmented Dickey-Fuller test
    :return:
    """
    results = adfuller(z, **kwargs)
    return results


def hurst_exp(z):
    """
    Hurst exponent computation
    :param z:
    :return:
    """
    hurst_val, p_value = genhurst(np.log(z))
    return hurst_val, p_value


def kf_simple(obs, obs_model):
    """
    Simple Kalman filter implementation.
    Parameters selected ad-hoc. For proper estimation see Rajamani and Rawlings (2007, 2009)
    y(t) = x(t)*beta(t) + e(t)
    beta(t) = beta(t-1) + w(t)
    :param obs:
    :param obs_model:
    :return: hidden variable, measurement error and its variance
    """

    x = np.stack((obs_model, np.ones(len(obs_model))), axis=1)
    param_dim = x.shape[1]

    # parameters of Kalman filter
    delta = 0.0001  # large delta gives quicker change in beta.
    Vw = delta / (1 - delta) * np.eye(param_dim)
    Ve = 0.001

    y_hat = np.full(obs.shape[0], np.nan)  # measurement prediction
    e = y_hat.copy()  # measurement error
    Q = y_hat.copy()  # variance-covariance matrix of e

    # For clarity, we denote R(t|t) by P(t). Initialize R, P and beta.
    R = np.zeros((param_dim, param_dim))  # variance-covariance matrix of beta: R(t|t-1)
    P = R.copy()  # variance-covariance matrix of beta: R(t|t)
    beta = np.full((param_dim, x.shape[0]), np.nan)

    # Initialize to zero
    beta[:, 0] = 0

    # Given initial beta and R (and P)
    for t in range(len(obs)):
        if t > 0:
            beta[:, t] = beta[:, t - 1]
            R = P + Vw

        y_hat[t] = np.dot(x[t, :], beta[:, t])
        Q[t] = np.dot(x[t, :], np.dot(R, x[t, :])) + Ve
        e[t] = obs[t] - y_hat[t]  # measurement prediction error
        K = np.dot(x[t, :], R) / Q[t]  # Kalman gain
        beta[:, t] = beta[:, t] + np.dot(K, e[t])  # State update. Equation 3.11
        P = R - np.dot(np.dot(K.reshape(-1, 1), x[t, :].reshape(-1, 1).T), R)  # State covariance update. Equation 3.12

    return beta, e, Q


def stats_print(time_idx, returns, rotation=0):
    """
    Computes APR and Sharpe ratio. Also plots cumulative returns of the strategy
    :param time_idx: time index
    :param returns: returns series
    :param rotation: angle to rotate xticks
    :return: None
    """
    cum_ret = (1 + returns).cumprod() - 1
    plt.plot(time_idx, cum_ret)
    plt.xticks(rotation=rotation)
    plt.show()

    print('APR={:.2f} and Sharpe={:.2f}'.format(np.prod(1 + returns) ** (252 / len(returns)) - 1, sharpe(returns, 252)))
    return
