"""
Mean Reversion Strategy on EWA and EWC ETF pairs using Kalman Filter.
Need to move to proper implementation. Also have to check the dimensionality of the matrices (i.e., unit tests)
"""

import numpy as np
import pandas as pd
import os
import utils
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()


def main():

    df = pd.read_csv(os.path.join(utils.PATH, 'inputData_EWA_EWC.csv'))
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    df.set_index('Date', inplace=True)

    x = df['EWA']
    y = df['EWC']
    beta, e, Q = utils.kf_simple(y, x)

    n_vars, n_periods,  = beta.shape[0] - 1, beta.shape[1]

    plt.plot(beta[0, 1:])
    plt.show()
    plt.plot(beta[1, 1:])
    plt.show()

    longs_entry = e < -np.sqrt(Q)
    longs_exit = e > 0

    shorts_entry = e > np.sqrt(Q)
    shorts_exit = e < 0

    num_units_long = np.full(longs_entry.shape, np.nan)
    num_units_short = np.full(shorts_entry.shape, np.nan)

    num_units_long[0] = 0
    num_units_long[longs_entry] = 1
    num_units_long[longs_exit] = 0
    num_units_long = pd.DataFrame(num_units_long)
    num_units_long.fillna(method='ffill', inplace=True)

    num_units_short[0] = 0
    num_units_short[shorts_entry] = -1
    num_units_short[shorts_exit] = 0
    num_units_short = pd.DataFrame(num_units_short)
    num_units_short.fillna(method='ffill', inplace=True)
    num_units = num_units_long + num_units_short

    # [hedgeRatio -ones(size(hedgeRatio))] is the shares allocation, [hedgeRatio -ones(size(hedgeRatio))].*y2
    # is the dollar capital allocation, while positions is the dollar capital in each ETF.
    positions = pd.DataFrame(np.tile(num_units.values, [1, 2]) * np.stack((-beta[:n_vars, :].flatten(),
                                                                          np.ones(n_periods)), axis=1) * df.values)
    returns = utils.returns_calculator(df, 1)
    pnl = utils.portfolio_return_calculator(positions, returns)  # daily P&L of the strategy
    ret = pnl / np.sum(np.abs(positions.shift()), axis=1)
    _ = utils.stats_print(df.index, ret)


if __name__ == "__main__":
    main()
