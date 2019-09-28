# Example 4.1: Post Earnings Announcement Drift (PEAD) strategy

import numpy as np
import pandas as pd
import os
import utils


def main():

    op = pd.read_csv(os.path.join(utils.PATH, 'inputDataOHLCDaily_20120424_op.csv'))
    cl = pd.read_csv(os.path.join(utils.PATH, 'inputDataOHLCDaily_20120424_cl.csv'))
    stocks = pd.read_csv(os.path.join(utils.PATH, 'inputDataOHLCDaily_20120424_stocks.csv'))

    op['Var1'] = pd.to_datetime(op['Var1'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    op.columns = np.insert(stocks.values, 0, 'Date')
    op.set_index('Date', inplace=True)

    cl['Var1'] = pd.to_datetime(cl['Var1'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    cl.columns = np.insert(stocks.values, 0, 'Date')
    cl.set_index('Date', inplace=True)

    earnann = pd.read_csv(os.path.join(utils.PATH, 'earnannFile.csv'))
    earnann['Date'] = pd.to_datetime(earnann['Date'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    earnann.set_index('Date', inplace=True)

    np.testing.assert_array_equal(stocks.iloc[0, :], earnann.columns)

    df = pd.merge(op, cl, how='inner', left_index=True, right_index=True, suffixes=('_op', '_cl'))
    df = pd.merge(earnann, df, how='inner', left_index=True, right_index=True)

    earnann = df.iloc[:, 0:(earnann.shape[1])].astype(bool)
    op = df.iloc[:, (earnann.shape[1]):((earnann.shape[1]) + op.shape[1])]
    cl = df.iloc[:, ((earnann.shape[1]) + op.shape[1]):]

    op.columns = stocks.iloc[0, :]
    cl.columns = stocks.iloc[0, :]

    lookback = 90

    retC2O = (op - cl.shift()) / cl.shift()
    stdC2O = retC2O.rolling(lookback).std()

    positions = np.zeros(cl.shape)

    longs = (retC2O >= 0.5 * stdC2O) & earnann
    shorts = (retC2O <= -0.5 * stdC2O) & earnann

    positions[longs] = 1
    positions[shorts] = -1

    ret = np.sum(positions * (cl - op) / op, axis=1) / 30
    cum_ret = utils.stats_print(cl.index, ret)

    max_dd, max_ddd = utils.drawdown_calculator(ret.values)
    print('Max DD=%f Max DDD in days=%i' % (max_dd, max_ddd ))
    # APR=0.068126 Sharpe=1.494743
    # Max DD=-0.026052 Max DDD in days=109


if __name__ == "__main__":
    main()
