# Example 2: Using ADF Test for Mean Reversion

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from genhurst import genhurst
import utils
sns.set()


def main():
    df = pd.read_csv(os.path.join(utils.PATH, 'inputData_USDCAD.csv'))
    y = df.loc[df['Time'] == 1659, 'Close']

    results = utils.adf_test(y, maxlag=1, regression='c', autolag=None)
    print(results)

    # Find Hurst exponent
    hurst_val, p_value = genhurst(np.log(y))
    print("H=%f pValue=%f" % (hurst_val, p_value))

    half_life = utils.half_life(y)
    print('Half life equals to {:.2f}'.format(half_life))

    lookback = np.round(half_life).astype(int)
    mkt_pos = -(y - y.rolling(lookback).mean()) / y.rolling(lookback).std()
    rets = utils.returns_calculator(y, 1)

    port_return = utils.portfolio_return_calculator(mkt_pos, rets)
    _ = utils.stats_print(np.arange(len(port_return)), port_return)


if __name__ == "__main__":
    main()
