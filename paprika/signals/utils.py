from statsmodels.tsa.stattools import coint, adfuller
from sklearn.linear_model import LinearRegression
from scipy.stats import pearsonr
import os
import numpy as np
import pandas as pd
import statsmodels.tsa.vector_ar.vecm as vm
import matplotlib.pyplot as plt
# from genhurst import genhurst

PATH = os.path.join(os.path.abspath(os.path.join(__file__, "../../..")), r"resources\data")


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
    df.reset_index(drop=True, inplace=True)
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


#def hurst_exp(z):
#    """
#    Hurst exponent computation
#    :param z:
##    :return:
#    """
#    hurst_val, p_value = genhurst(np.log(z))
#    return hurst_val, p_value


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
    :return: cumulative returns
    """
    cum_ret = (1 + returns).cumprod() - 1
    plt.plot(time_idx, cum_ret)
    plt.xticks(rotation=rotation)
    plt.show()

    print('APR={:.2f} and Sharpe={:.2f}'.format(np.prod(1 + returns) ** (252 / len(returns)) - 1, sharpe(returns, 252)))
    max_dd, max_dd_duration = drawdown_calculator(returns)
    print('Maximum Drawdown={:.2f} and Maximum Drawdown Duration={:.2f}'.format(max_dd, max_dd_duration))
    return cum_ret
