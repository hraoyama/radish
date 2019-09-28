# Example 4.2: Arbitrage between SPY and Its Component Stocks

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import utils


def main():

    # Stocks
    cl = pd.read_csv(os.path.join(utils.PATH, 'inputDataOHLCDaily_20120424_cl.csv'))
    stocks = pd.read_csv(os.path.join(utils.PATH, 'inputDataOHLCDaily_20120424_stocks.csv'))

    cl['Var1'] = pd.to_datetime(cl['Var1'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    cl.columns = np.insert(stocks.values, 0, 'Date')
    cl.set_index('Date', inplace=True)

    # ETFs
    cl_etf = pd.read_csv(os.path.join(utils.PATH, 'inputData_ETF_cl.csv'))
    etfs = pd.read_csv(os.path.join(utils.PATH, 'inputData_ETF_stocks.csv'))

    cl_etf['Var1'] = pd.to_datetime(cl_etf['Var1'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    cl_etf.columns = np.insert(etfs.values, 0, 'Date')
    cl_etf.set_index('Date', inplace=True)

    # Merge on common dates
    df = pd.merge(cl, cl_etf, how='inner', on='Date')

    cl_stocks = df[cl.columns]
    cl_etf = df[cl_etf.columns]

    # Use SPY only
    cl_etf = cl_etf['SPY']  # This turns cl_etf into Series

    train_idx = df.index[
        (df.index > pd.datetime(2007, 1, 1).date()) & (df.index <= pd.datetime(2007, 12, 31).date())]
    test_idx = df.index[df.index > pd.datetime(2007, 12, 31).date()]

    is_cointegrated = np.full(stocks.shape[1], False)
    for i, ticker in enumerate(stocks.values[0]):
        # Combine the two time series into a matrix y2 for input into Johansen test
        y2 = pd.concat([cl_stocks.loc[train_idx, ticker], cl_etf.loc[train_idx]], axis=1)
        y2 = y2[y2.notnull().all(axis=1)]

        if y2.shape[0] > 250:
            # Johansen test
            result = utils.johansen_test(y2.values, det_order=0, k_ar_diff=1)
            if result.lr1[0] > result.cvt[0, 0]:
                is_cointegrated[i] = True

    print(is_cointegrated.sum())

    yn = cl_stocks.loc[train_idx, is_cointegrated]
    # The net market value of the long-only portfolio is same as the "spread"
    log_mktval_long = np.sum(np.log(yn), axis=1)

    # Confirm that the portfolio cointegrates with SPY
    ytest = pd.concat([log_mktval_long, np.log(cl_etf.loc[train_idx])], axis=1)
    result = utils.johansen_test(ytest.values, det_order=0, k_ar_diff=1)
    print(result.lr1)
    print(result.cvt)
    print(result.lr2)
    print(result.cvm)

    # Apply linear mean-reversion model on test set

    # Array of stock and ETF prices
    ynplus = pd.concat([cl_stocks.loc[test_idx, is_cointegrated], pd.DataFrame(cl_etf.loc[test_idx])], axis=1)
    # Array of log market value of stocks and ETF
    weights = np.column_stack((np.full((test_idx.shape[0], is_cointegrated.sum()), result.evec[0, 0]),
                               np.full((test_idx.shape[0], 1), result.evec[1, 0])))

    log_mkt_val = np.sum(weights * np.log(ynplus), axis=1)  # Log market value of long-short portfolio

    lookback = 5
    num_units = -(log_mkt_val - log_mkt_val.rolling(lookback).mean()) / log_mkt_val.rolling(lookback).std()
    # capital invested in portfolio in dollars.
    positions = pd.DataFrame(np.expand_dims(num_units, axis=1) * weights)
    returns = utils.returns_calculator(ynplus, 1)
    pnl = utils.portfolio_return_calculator(positions, returns)  # daily P&L of the strategy
    ret = pnl / np.sum(np.abs(positions.shift(1)), axis=1)

    ret[ret.isnull()] = 0
    _ = utils.stats_print(test_idx, ret, rotation=45)


if __name__ == "__main__":
    main()
