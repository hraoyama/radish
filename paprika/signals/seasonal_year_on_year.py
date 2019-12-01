# Backtesting a Year-on-Year Seasonal Trending Strategy

import numpy as np
import pandas as pd
import os
import utils


def main():

    df = pd.read_table(os.path.join(utils.PATH, 'SPX_20071123.txt'))
    df['Date'] = df['Date'].round().astype('int')
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    df.set_index('Date', inplace=True)

    # End of month prices. Need to remove last date because it isn't really end of January.
    end_of_month_prices = df.resample('M').last()[:-1]
    monthly_returns = utils.returns_calculator(end_of_month_prices, 1)

    positions = np.zeros(monthly_returns.shape)

    for m in range(13, monthly_returns.shape[0]):
        hasData = np.where(np.isfinite(monthly_returns.iloc[m-12, :]))[0]
        sort_index = np.argsort(monthly_returns.iloc[m-12, hasData])
        badData = np.where(np.logical_not(np.isfinite(monthly_returns.iloc[m, hasData[sort_index]])))[0]
        sort_index = sort_index[~sort_index.isin(badData)]
        topN = int(len(sort_index)/10)
        positions[m-1, hasData[sort_index.values[np.arange(topN)]]] = -1
        positions[m-1, hasData[sort_index.values[np.arange(-topN, 0)]]] = +1

    capital = np.nansum(np.array(pd.DataFrame(abs(positions)).shift()), axis=1)
    positions[capital == 0, ] = 0
    capital[capital == 0] = 1
    ret = np.nansum(np.array(pd.DataFrame(positions).shift()) * np.array(monthly_returns), axis=1) / capital
    avgret = np.nanmean(ret) * 12
    sharpe = np.sqrt(12)*np.nanmean(ret)/np.nanstd(ret)
    print('Avg ann return=%f Sharpe ratio=%f' % (avgret, sharpe))


if __name__ == "__main__":
    main()
