# Example 5.4: Mean Reversion Trading of Calendar Spreads
import numpy as np
import pandas as pd
import os
import utils
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm
import matplotlib.pyplot as plt


def main():

    df = pd.read_csv(os.path.join(utils.PATH, 'inputDataDaily_CL_20120502.csv'))
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    df.set_index('Date', inplace=True)

    # Fitting gamma to forward curve
    gamma = np.full(df.shape[0], np.nan)
    for t in range(df.shape[0]):
        idx = np.where(np.isfinite(df.iloc[t, :]))[0]
        idx_diff = idx[1:] - idx[:-1]
        if (len(idx) >= 5) & all(idx_diff[:4] == 1):
            FT = df.iloc[t, idx[:5]]
            model = LinearRegression().fit(np.array(range(FT.shape[0])).reshape(-1, 1), np.log(FT.values))
            gamma[t] = -12 * model.coef_[0]

    plt.plot(gamma)
    plt.show()

    filter_nans = np.isfinite(gamma)

    results = utils.adf_test(gamma[np.where(filter_nans)[0]], maxlag=1, regression='c', autolag=None)
    print(results)

    gamma = pd.Series(gamma)
    half_life = utils.half_life(gamma[np.where(filter_nans)[0]])

    lookback = int(half_life)
    ma = gamma.rolling(lookback).mean()
    mstd = gamma.rolling(lookback).std()
    zscore = (gamma - ma) / mstd

    positions = np.zeros(df.shape)
    isExpireDate = np.isfinite(df) & ~np.isfinite(df.shift(-1))
    holddays = 3 * 21
    numDaysStart = holddays + 10
    numDaysEnd = 10
    spreadMonth = 12

    for c in range(0, df.shape[1] - spreadMonth):
        expireIdx = np.where(isExpireDate.iloc[:, c])[-1]
        if c == 0:
            startIdx = max(0, expireIdx - numDaysStart)
            endIdx = expireIdx - numDaysEnd
        else:
            myStartIdx = endIdx + 1
            myEndIdx = expireIdx - numDaysEnd
            if (myEndIdx - myStartIdx >= holddays):
                startIdx = myStartIdx
                endIdx = myEndIdx
            else:
                startIdx = np.Inf

        if ((len(expireIdx) > 0) & (endIdx > startIdx)):
            positions[startIdx[0]:endIdx[0], c] = -1
            positions[startIdx[0]:endIdx[0], c + spreadMonth] = 1

    positions[zscore.isna().values.flatten(), :] = 0
    zscore.fillna(-np.Inf, inplace=True)

    positions[zscore.values.flatten() > 0, :] = -positions[zscore.values.flatten() > 0, :]
    positions = pd.DataFrame(positions)
    returns = utils.returns_calculator(df, 1)
    pnl = utils.portfolio_return_calculator(positions, returns)  # daily P&L of the strategy
    ret = pnl / np.sum(np.abs(positions.shift(1)), axis=1)
    _ = utils.stats_print(df.index, ret)
    # APR=0.024347 Sharpe=1.275860


if __name__ == "__main__":
    main()
