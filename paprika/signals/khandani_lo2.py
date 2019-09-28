# Example 4.3: Linear Long-Short Model on Stocks

import numpy as np
import pandas as pd
import utils
import os
import matplotlib.pyplot as plt


def main():

    # Stocks
    cl_ = pd.read_csv(os.path.join(utils.PATH, 'inputDataOHLCDaily_20120424_cl.csv'))
    stocks = pd.read_csv(os.path.join(utils.PATH, 'inputDataOHLCDaily_20120424_stocks.csv'))

    cl_['Var1'] = pd.to_datetime(cl_['Var1'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    cl_.columns = np.insert(stocks.values, 0, 'Date')
    cl_.set_index('Date', inplace=True)

    cl_ = cl_.loc[(cl_.index >= pd.datetime(2007, 1, 3).date()) & (cl_.index <= pd.datetime(2011, 12, 30).date()), :]

    op = pd.read_csv(os.path.join(utils.PATH, 'inputDataOHLCDaily_20120424_op.csv'))
    op['Var1'] = pd.to_datetime(op['Var1'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    op.columns = np.insert(stocks.values, 0, 'Date')
    op.set_index('Date', inplace=True)

    op = op.loc[(op.index >= pd.datetime(2007, 1, 3).date()) & (op.index <= pd.datetime(2011, 12, 30).date()), :]

    returns = utils.returns_calculator(cl_, 1)  # daily returns
    market = returns.mean(axis=1)  # equal weighted market index return

    weights = -(returns.T - market).T
    scaler = np.nansum(np.abs(weights), axis=1)
    scaler[scaler == 0] = 1
    weights = (weights.T / scaler).T

    daily_return = utils.portfolio_return_calculator(weights, returns)  # Capital is always one
    _ = utils.stats_print(cl_.index, daily_return)

    returns = (op - cl_.shift(1)) / cl_.shift(1)  # daily returns
    market = returns.mean(axis=1)  # equal weighted market index return

    weights = -(returns.T - market).T
    scaler = np.nansum(np.abs(weights), axis=1)
    scaler[scaler == 0] = 1
    weights = (weights.T / scaler).T

    daily_return = (weights * (cl_ - op) / op).sum(axis=1)  # Capital is always one
    _ = utils.stats_print(cl_.index, daily_return)


if __name__ == "__main__":
    main()
