# Example 7.1: Opening Gap Strategy for Dow Jones STOXX 50 index futures (FSTX) trading on Eurex
# This one is not profitable, but can be used as one of the signals
# Mean reversion works though

import numpy as np
import pandas as pd
import utils
import os


def main():

    entry_zscore = 0.1
    df = pd.read_csv(os.path.join(utils.PATH, 'inputDataDaily_FSTX_20120517.csv'))
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d').dt.date  # remove HH:MM:SS
    df.set_index('Date', inplace=True)

    vol_estimate = utils.returns_calculator(df['Close'], 1).rolling(90).std().shift(1)

    longs = df['Open'] >= (df['High'].shift(1) * (1 + entry_zscore * vol_estimate))
    shorts = df['Open'] <= (df['Low'].shift(1) * (1 - entry_zscore * vol_estimate))

    positions = np.zeros(longs.shape)
    positions[longs] = -1
    positions[shorts] = 1
    ret = positions * (df['Close'] - df['Open']) / df['Open']
    ret[ret.isnull()] = 0

    _ = utils.stats_print(df.index, ret)
    max_dd, max_dd_dur = utils.drawdown_calculator(ret.values)

    print('Max DD=%f Max DDD in days=%i' % (max_dd, max_dd_dur))


if __name__ == "__main__":
    main()
