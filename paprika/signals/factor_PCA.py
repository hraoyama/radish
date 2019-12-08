from sklearn.linear_model import LinearRegression
import pandas as pd
import os
import numpy as np
from paprika.utils import utils


def main():

    ts = pd.read_table(os.path.join(utils.PATH, 'IJR_20080114.txt'))  # S&P 600 small-cap stocks
    ts['Date'] = pd.to_datetime(ts['Date'].map(np.round), format='%Y%m%d')
    # do not forget to sort by date
    ts = ts.sort_values(by=['Date'])
    ts.set_index('Date', inplace=True)
    ts.fillna(method='ffill', inplace=True)  # just following Ernie here
    daily_returns = utils.returns_calculator(ts, 1)

    # model parameters:
    lookback = 252
    assets_in_strategy = 50
    n_factors = 5
    n_periods, _ = daily_returns.shape

    positions = np.zeros(daily_returns.shape)

    for i in range(1, n_periods - lookback + 1):
        sample = daily_returns[i:i+lookback].T
        keep_filter = np.where(sample.notnull().all(axis=1))[0]
        n_active = len(keep_filter)
        sample = (sample.iloc[keep_filter, :]).T

        mean = sample.mean(axis=0)
        sample -= mean
        cov_matrix = np.cov(sample.T)
        eigen_values, eigen_vectors = np.linalg.eigh(cov_matrix)
        fit_obj = LinearRegression(fit_intercept=False).fit(eigen_vectors[:, n_active-n_factors:], sample.iloc[-1, :])
        beta = fit_obj.coef_

        exp_returns = mean + np.dot(eigen_vectors[:, n_active-n_factors:], beta)
        sort_indices = np.argsort(exp_returns)
        positions[i+lookback-1, keep_filter[sort_indices[:assets_in_strategy]]] = -1
        positions[i+lookback-1, keep_filter[sort_indices[n_active-assets_in_strategy:]]] = 1

    port_return = utils.portfolio_return_calculator(positions, daily_returns)
    sharpe_ratio = utils.sharpe(port_return, 252)
    print('Sharpe ratio of the PCA factor return strategy on the whole set is {:.2f}'.format(sharpe_ratio))


if __name__ == "__main__":
    main()
