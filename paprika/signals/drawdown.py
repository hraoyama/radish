"""
Computes Sharpe ratio and drawdowns for
long-only portfolio with IGE
long-short market-neutral portfolio (long IGE, short SPY)
"""

import utils
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sn
import os
sn.set()



def main():
    ts1 = pd.read_excel(os.path.join(utils.PATH, 'IGE.xls'))
    ts1 = ts1.sort_values(by=['Date'])
    ts1 = ts1.reset_index(drop=True)
    ts1['ret'] = utils.returns_calculator(ts1['Adj Close'], 1)
    risk_free = 0.04 / 252
    ts1['net'] = ts1['ret'] - risk_free

    sharpe_long_only = utils.sharpe(ts1['net'], 252)
    print(sharpe_long_only)

    ts2 = pd.read_excel(os.path.join(utils.PATH, 'SPY.xls'))
    ts2 = ts2.sort_values(by=['Date'])
    ts2 = ts2.reset_index(drop=True)
    ts2['ret'] = utils.returns_calculator(ts2['Adj Close'], 1)
    # here we short an equal dollar amount of SPY as hedge
    # divide by 2 because we have double amount of capital
    ts2['net'] = (ts1['ret'] - ts2['ret']) / 2

    sharpe_market_neutral = utils.sharpe(ts2['net'], 252)
    print(sharpe_market_neutral)

    max_drawdown, max_drawdown_duration = utils.drawdown_calculator(ts2['net'])
    print(max_drawdown, max_drawdown_duration)

    ts2['cum_ret'] = (1 + ts2['net']).cumprod() - 1
    plt.plot(ts2['Date'], ts2['cum_ret'])
    plt.show()


if __name__ == "__main__":
    main()
