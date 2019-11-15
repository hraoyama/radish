from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.signals.simple_kalman_filter import SimpleKalmanSignal
from paprika.utils import utils

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()


def test_trading_spread():

    tickers = ["EWA", "EWC"]
    ewa_ewc_feed = Feed('EWA_EWC_kalman', datetime(2000, 7, 1), datetime(2020, 1, 1))
    ewa_ewc_feed.set_feed(tickers, DataType.OHLCVAC_PRICE, how='inner')

    ewa_ewc_signal = SimpleKalmanSignal(delta=0.0001, Ve=0.001, Y_NAME="EWC", X_NAME="EWA")
    ewa_ewc_feed.add_subscriber(ewa_ewc_signal)

    ewa_ewc_signal.run()

    ewa_ewc_positions, ewa_ewc_prices = ewa_ewc_signal.positions, ewa_ewc_signal.prices
    ewa_ewc_returns = utils.returns_calculator(ewa_ewc_prices[[ewa_ewc_signal.y_name, ewa_ewc_signal.x_name]], 1)
    ewa_ewc_pnl = utils.portfolio_return_calculator(ewa_ewc_positions, ewa_ewc_returns)  # daily P&L of the strategy
    strategy_return = ewa_ewc_pnl / np.sum(np.abs(ewa_ewc_positions.shift()), axis=1)
    _ = utils.stats_print(ewa_ewc_prices['DateTime'], strategy_return)