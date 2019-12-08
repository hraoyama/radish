# Backtesting the January Effect

import numpy as np
import pandas as pd
import os
from paprika.utils import utils


def main():

    onewaytcost=0.0005

    df = pd.read_table(os.path.join(utils.PATH, 'IJR_20080131.txt'))
    df['Date'] = df['Date'].round().astype('int')
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    df.set_index('Date', inplace=True)

    # End of December prices. Need to remove last date because it isn't really end of year
    end_of_year_prices = df.resample('Y').last()[0:-1]
    annual_returns = utils.returns_calculator(end_of_year_prices, 1)[1:]  # first row has NaN

    # End of January prices. Need to remove first date to match the years in lastdayofDec.
    # Need to remove last date because it isn't really end of January.
    end_of_jan_prices = df.resample('BA-JAN').last()[1:-1]
    jan_returns = (end_of_jan_prices.values-end_of_year_prices.values) / end_of_year_prices.values
    jan_returns = jan_returns[1:, ]  # match number of rows in annual_returns

    for y in range(len(annual_returns)):
        hasData = np.where(np.isfinite(annual_returns.iloc[y, :]))[0]
        sort_index = np.argsort(annual_returns.iloc[y, hasData].values)
        topN = int(len(hasData)/10)
        # portfolio returns
        portfolio_return = (np.nanmean(jan_returns[y, hasData[sort_index[np.arange(topN)]]]) -
                                np.nanmean(jan_returns[y, hasData[sort_index[np.arange(-topN, 0)]]]))/2-2*onewaytcost

        print('Last holding date %s: Portfolio return=%f' % (end_of_jan_prices.index[y], portfolio_return))


if __name__ == "__main__":
    main()
