    # Example 5.1: Pair Trading USD.AUD vs USD.CAD Using the Johansen Eigenvector

import numpy as np
import pandas as pd
import os
from paprika.utils import utils


def main():

    df1 = pd.read_csv(os.path.join(utils.PATH, 'inputData_USDCAD_20120426.csv'))
    df1['Date'] = pd.to_datetime(df1['Date'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    df1.rename(columns={'Close': 'CAD'}, inplace=True)
    df1['CAD'] = 1 / df1['CAD']

    df2 = pd.read_csv(os.path.join(utils.PATH, 'inputData_AUDUSD_20120426.csv'))
    df2['Date'] = pd.to_datetime(df2['Date'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    df2.rename(columns={'Close': 'AUD'}, inplace=True)

    df = pd.merge(df1, df2, how='inner', on='Date')
    df.set_index('Date', inplace=True)

    train_period = 250
    lookback = 20
    t_periods, n_asset = df.shape

    hedge_ratio = np.full((t_periods, n_asset), np.NaN)
    num_units = np.full(t_periods, np.NaN)

    for t in range(train_period + 1, t_periods):
        # Johansen test
        result = utils.johansen_test(df.values[(t - train_period - 1):t - 1], det_order=0, k_ar_diff=1)
        hedge_ratio[t, :] = result.evec[:, 0]
        yport = pd.DataFrame(np.dot(df.values[(t - lookback):t], result.evec[:, 0]))  # (net) market value of portfolio
        ma = yport.mean()
        mstd = yport.std()
        num_units[t] = -(yport.iloc[-1, :] - ma) / mstd

    positions = pd.DataFrame(np.expand_dims(num_units, axis=1) * hedge_ratio) * df.values
    returns = utils.returns_calculator(df, 1)
    pnl = utils.portfolio_return_calculator(positions, returns)  # daily P&L of the strategy
    ret = pnl / np.sum(np.abs(positions.shift(1)), axis=1)
    _ = utils.stats_print(df.index, ret, rotation=45)


if __name__ == "__main__":
    main()
