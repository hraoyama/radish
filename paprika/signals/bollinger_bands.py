# Trading Price Spread using Bollinger Bands
from sklearn.linear_model import LinearRegression

import utils
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def main():
    df = pd.read_csv(os.path.join(utils.PATH, 'inputData_GLD_USO.csv'))
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    df.set_index('Date', inplace=True)

    lookback = 20
    n_periods = df.shape[0]
    hedge_ratio = np.full(n_periods, np.nan)
    for t in np.arange(lookback, n_periods+1):
        regress_results = LinearRegression().fit(df.iloc[(t-lookback):t]['GLD'].values.reshape(-1, 1),
                                                 df.iloc[(t-lookback):t]['USO'].values)
        hedge_ratio[t-1] = regress_results.coef_[0]

    portfolio_value = np.sum(np.stack((-hedge_ratio, np.ones(n_periods)), axis=1) * df[['GLD', 'USO']], axis=1)
    portfolio_value.plot()
    plt.show()

    # trimming off lookback period
    portfolio_value = portfolio_value[lookback:]
    hedge_ratio = hedge_ratio[lookback:]
    df = df[lookback:]
    n_periods = df.shape[0]

    z_scores = (portfolio_value - portfolio_value.rolling(lookback).mean()) / portfolio_value.rolling(lookback).std()

    longs_entry = -1
    longs_exit = 0

    shorts_entry = 1
    shorts_exit = 0

    num_units_long = np.full(n_periods, np.nan)
    num_units_short = np.full(n_periods, np.nan)

    num_units_long[0] = 0
    num_units_long[z_scores < longs_entry] = 1
    num_units_long[z_scores >= longs_exit] = 0
    num_units_long = pd.DataFrame(num_units_long)
    num_units_long.fillna(method='ffill', inplace=True)

    num_units_short[0] = 0
    num_units_short[z_scores > shorts_entry] = -1
    num_units_short[z_scores <= shorts_exit] = 0
    num_units_short = pd.DataFrame(num_units_short)
    num_units_short.fillna(method='ffill', inplace=True)
    num_units = num_units_long + num_units_short

    # capital invested in portfolio in dollars
    positions = pd.DataFrame(np.tile(num_units.values, [1, 2]) * np.stack((-hedge_ratio,
                                                                             np.ones(n_periods)), axis=1) *
                             df[['GLD', 'USO']].values)
    returns = utils.returns_calculator(df[['GLD', 'USO']], 1)
    pnl = utils.portfolio_return_calculator(positions, returns)  # daily P&L of the strategy
    ret = pnl / np.sum(np.abs(positions.shift()), axis=1)
    ret[ret.isnull()] = 0
    _ = utils.stats_print(df.index, ret)


if __name__ == "__main__":
    main()
