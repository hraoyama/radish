"""
Cointegration strategy between triplet of ETFs (EWA, EWC, IGE)
"""

import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import utils
import seaborn as sns
sns.set()


def main():

    df = pd.read_csv(os.path.join(utils.PATH, 'inputData_EWA_EWC_IGE.csv'))
    df['Date'] = pd.to_datetime(df['Date'],  format='%Y%m%d').dt.date  # remove HH:MM:SS
    df.set_index('Date', inplace=True)

    # df.plot()
    # df.plot.scatter(x='EWA', y='EWC')
    # plt.show()

    results = LinearRegression().fit(df['EWA'].values.reshape(-1, 1), df['EWC'])
    hedge_ratio = results.coef_[0]
    print('hedgeRatio=%f' % hedge_ratio)

    coint_t, pvalue, _ = utils.cadf_test(df['EWC'], df['EWA'])
    print('t-statistic - {} and p-value - {}'.format(coint_t, pvalue))

    result = utils.johansen_test(df[['EWA', 'EWC']].values, det_order=0, k_ar_diff=1)
    print('Trace statistic - {} and critical values - {}'.format(result.lr1, result.cvt))
    print('Trace statistic - {} and critical values - {}'.format(result.lr2, result.cvm))

    result = utils.johansen_test(df.values, det_order=0, k_ar_diff=1)
    print('Trace statistic - {} and critical values - {}'.format(result.lr1, result.cvt))
    print('Trace statistic - {} and critical values - {}'.format(result.lr2, result.cvm))

    # (net) market value of portfolio
    yport = pd.Series(np.dot(df.values, result.evec[:, 0]))
    half_life = utils.half_life(yport)
    print('Half life = {} days'.format(half_life))

    #  Apply a simple linear mean reversion strategy to EWA-EWC-IGE
    lookback = np.round(half_life).astype(int)  # setting lookback to the halflife found above
    # Capital invested in portfolio in dollars.
    num_units = -(yport - yport.rolling(lookback).mean()) / yport.rolling(lookback).std()

    # results.evec(:, 1)' can be viewed as the capital allocation, while positions is the dollar capital in each ETF.
    positions = pd.DataFrame(np.dot(num_units.values.reshape(-1, 1), np.expand_dims(result.evec[:, 0], axis=1).T)
                             * df.values)
    returns = utils.returns_calculator(df, 1)

    pnl = utils.portfolio_return_calculator(positions, returns)  # daily P&L of the strategy
    ret = pnl / np.sum(np.abs(positions.shift()), axis=1)
    _ = utils.stats_print(df.index, ret)


if __name__ == "__main__":
    main()
