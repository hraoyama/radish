# Trading Price Spread
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

    for t in np.arange(lookback, n_periods + 1):
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

    num_units = -(portfolio_value - portfolio_value.rolling(lookback).mean()) / portfolio_value.rolling(lookback).std()
    # capital invested in portfolio in dollars
    positions = pd.DataFrame(np.tile(num_units.values, [2, 1]).T * np.stack((-hedge_ratio, np.ones(n_periods)), axis=1)
                             * df[['GLD', 'USO']].values)
    returns = utils.returns_calculator(df[['GLD', 'USO']], 1)
    pnl = utils.portfolio_return_calculator(positions, returns)  # daily P&L of the strategy
    ret = pnl / np.sum(np.abs(positions.shift()), axis=1)
    ret[ret.isnull()] = 0
    _ = utils.stats_print(df.index, ret)


if __name__ == "__main__":
    main()
