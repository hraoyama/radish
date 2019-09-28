import utils
import pandas as pd
import numpy as np
import os


def main():
    # ETF on oil service
    ts1 = pd.read_excel(os.path.join(utils.PATH, 'OIH.xls'), usecols=[0, 6])
    # ETF on retail bank
    ts2 = pd.read_excel(os.path.join(utils.PATH, 'RKH.xls'), usecols=[0, 6])

    ts = pd.merge(ts1, ts2, on='Date', suffixes=('_OIH', '_RKH'))
    # ETF on retail
    ts3 = pd.read_excel(os.path.join(utils.PATH, 'RTH.xls'), usecols=[0, 6])
    ts = pd.merge(ts, ts3, on='Date')
    ts.set_index('Date', inplace=True)
    ts.sort_index(inplace=True)

    rets = utils.returns_calculator(ts, 1)
    # drop any returns that have NaN on any day
    rets = rets[~np.any(np.isnan(rets), axis=1)]
    risk_free = 0.04 / 252
    rets -= risk_free

    # annualized returns
    M = 252 * np.mean(rets, axis=0)
    C = 252 * np.cov(rets.T) # need to transpose matrix

    # Kelly optimal leverages
    F = np.dot(np.linalg.inv(C), M)

    # maximum compounded growth rate of a multi-strategy Gaussian process is
    g = 252 * risk_free + 0.5 * np.dot(F, np.dot(C, F))
    # the respective Sharpe ratio
    sharp = np.sqrt(np.dot(F, np.dot(C, F)))

    print("Sharpe ratio with Kelly allocation strategy before adjusting for transaction costs is {:.2f}.".format(sharp))


if __name__ == "__main__":
    main()
