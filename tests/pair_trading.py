from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from absl import app, logging
import math
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm

import os, sys
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')
# sys.path.insert(0, myPath)

from paprika.core.algo_runner import RunMode, setup_runner
from paprika.core.algorithm import Algorithm
from paprika.core.api import (get_ohlcv, get_tickers, order_target_percent,
                              register_timer, get_markets, get_orderbook,
                              get_my_trades, get_portfolio, get_open_orders,
                              cancel_order, order_target_percents)
from paprika.core.config import BacktestConfig
from paprika.data.fetcher import HistoricalDataFetcher
from paprika.portfolio.account_type import AccountType

np.set_printoptions(suppress=True)

# Set up algo
source = 'mdb'
asset = 'USD'
currency = 'USD'
frequency = '1d'


class PairTrading(Algorithm):
    def __init__(self, look_back=100):
        self._look_back = 100

    def initialise(self):
        register_timer(self.handle_timer, frequency)

    def zscore(series):
        return (series - series.mean()) / np.std(series)

    def handle_timer(self, event):
        portfolio = get_portfolio(source)
        ticker1 = "EUX.FBTP201709"
        ticker2 = "EUX.FBTS201709"
        timestamps, prices1 = get_ohlcv(
            source=source,
            symbol=ticker1,
            frequency=frequency,
            fields=['close'],
            limit=self._look_back)
        _, prices2 = get_ohlcv(
            source=source,
            symbol=ticker2,
            frequency=frequency,
            fields=['close'],
            limit=self._look_back)

        score, pvalue, _ = coint(prices1, prices2)

        if pvalue < 0.05:
            prices1 = sm.add_constant(prices1)
            results = sm.OLS(prices2, prices1).fit()


def main(argv):
    # Backtest config
    start = datetime(2018, 1, 29)
    end = datetime(2018, 2, 24)
    start_fund = 2e5
    config = BacktestConfig(start_datetime=start, end_datetime=end, frequency=frequency)
    config.frequency = frequency
    config.add_initial_portfolio(
        source=source,
        asset=asset,
        amount=start_fund,
        account_type=AccountType.EXCHANGE,
        base_currency=currency)

    # Run
    with setup_runner(PairTrading(), RunMode.BACKTEST, config) as runner:
        result = runner.run()

    pass


if __name__ == '__main__':
    app.run(main)



