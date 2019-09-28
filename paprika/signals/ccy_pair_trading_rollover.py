# Example 5.2: Pair Trading AUD.CAD with Rollover Interests
# The signal generation is not quite clear and consistent with ccy_pair_trading example

import numpy as np
import pandas as pd
import os
import utils


def main():

    df = pd.read_csv(os.path.join(utils.PATH, 'inputData_AUDCAD_20120426.csv'))
    df['Date'] = pd.to_datetime(df['Date'],  format='%Y%m%d')
    df.set_index('Date', inplace=True)

    aud = pd.read_csv(os.path.join(utils.PATH, 'AUD_interestRate.csv'))
    aud_index = pd.PeriodIndex(year=aud.Year, month=aud.Month, freq='M')
    aud.index = aud_index.to_timestamp()

    cad = pd.read_csv(os.path.join(utils.PATH, 'CAD_interestRate.csv'))
    cad_index = pd.PeriodIndex(year=cad.Year, month=cad.Month, freq='M')
    cad.index = cad_index.to_timestamp()

    df = pd.merge(df, aud, how='outer', left_index=True, right_index=True)
    df.drop({'Year', 'Month'}, axis=1, inplace=True)
    df.rename({'Rates': 'AUD_Rates'}, axis=1, inplace=True)

    df = pd.merge(df, cad, how='outer', left_index=True, right_index=True)
    df.drop({'Year', 'Month'}, axis=1, inplace=True)
    df.rename({'Rates': 'CAD_Rates'}, axis=1, inplace=True)

    df.fillna(method='ffill', axis=0, inplace=True)
    # convert from annual to daily rates
    df.loc[:, ['AUD_Rates', 'CAD_Rates']] = df.loc[:, ['AUD_Rates', 'CAD_Rates']] / 365 / 100

    isWednesday = df.index.weekday == 2
    df.loc[isWednesday, 'AUD_Rates'] = df.loc[isWednesday, 'AUD_Rates'] * 3

    isThursday = df.index.weekday == 3
    df.loc[isThursday, 'CAD_Rates'] = df.loc[isThursday, 'CAD_Rates'] * 3

    lookback = 20

    ma = df['Close'].rolling(lookback).mean()
    z = -(df['Close'] - ma)

    ret = np.sign(z).shift(1) * (np.log(df['Close']/df['Close'].shift(1)*(1 + df['AUD_Rates'])/(1 + df['CAD_Rates'])))
    _ = utils.stats_print(df.index, ret, rotation=45)
    # APR=0.064719 Sharpe=0.610818


if __name__ == "__main__":
    main()
