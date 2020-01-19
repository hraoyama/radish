# Test of USO-GLD trading price spread
from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.signals.signals.scaling_in import ScalingInLevels
from paprika.utils import utils
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()


def test_scaling_in_gold_spread():

    DataChannel.clear_redis()

    tickers = ["GOLD2", "USO"]
    gold_uso_feed = Feed('GOLD_USO_SPREAD', datetime(2000, 7, 1), datetime(2020, 1, 1))
    gold_uso_feed.set_feed(tickers, DataType.OHLCVAC_PRICE, how='inner')

    gold_uso_signal = ScalingInLevels(LOOKBACK=20, Y_NAME="USO", X_NAME="GOLD2")
    gold_uso_feed.add_subscriber(gold_uso_signal)

    gold_uso_signal.run()
    gold_uso_spreads = gold_uso_signal.spreads[gold_uso_signal.lookback:]
    gold_uso_positions = gold_uso_signal.positions[gold_uso_signal.lookback:]
    gold_uso_prices = gold_uso_signal.prices[gold_uso_signal.lookback:]
    plt.plot(gold_uso_prices['DateTime'], gold_uso_spreads)
    plt.show()

    column_names = [gold_uso_signal.y_name, gold_uso_signal.x_name]
    gold_uso_returns = utils.returns_calculator(gold_uso_prices[column_names], 1)
    # daily P&L of the strategy
    gold_uso_pnl = utils.portfolio_return_calculator(gold_uso_positions[column_names], gold_uso_returns)
    strategy_return = gold_uso_pnl / np.sum(np.abs(gold_uso_positions[column_names].shift()), axis=1)
    strategy_return[strategy_return.isnull()] = 0
    _ = utils.stats_print(gold_uso_prices['DateTime'], strategy_return)
