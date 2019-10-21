from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.data.feed_filter import TimeFreqFilter, Filtration, TimePeriod
from paprika.signals.signal_cointegration import CointegrationSpread
from paprika.utils import utils

from datetime import datetime
from pprint import pprint as pp
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def test_cointegration_signal():
    
    # # in order to explore the data in advance
    # from paprika.data.data_channel import DataChannel
    # DataChannel.check_register(["ETF.FR0007054358","ETF.DE0005933923"], feeds_db=False)

    tickers = ["ETF.FR0007054358", "ETF.DE0005933923"]

    # set up the pairs data you will be looking at
    feed = Feed('CG', datetime(2018, 1, 30), datetime(2019, 3, 30))
    feed.set_feed(tickers, DataType.TRADES)

    # how much data did we actually get?
    pp(feed.shape)

    # this is a crazy delay, but this is just so we have trades
    signal = CointegrationSpread(BETA=1.631, MEAN=0.052196, STD=1.9487, ENTRY=1, EXIT=0.55,
                                 Y_NAME="ETF.FR0007054358", X_NAME="ETF.DE0005933923",
                                 MAX_TIME_DISTANCE=pd.Timedelta(np.timedelta64(600, 's')))
    signal.add_filtration(Filtration(TimeFreqFilter(TimePeriod.SECOND, 600)))
    feed.add_subscriber(signal)

    # execute the signal
    signal.run()

    # show some of your results
    print(signal.positions)

    positions = signal.positions[tickers].fillna(method='ffill').values
    prices = signal.prices

    train_idx = 252  # window where above parameters were estimated

    returns = utils.returns_calculator(prices[tickers], 1)
    port_return = utils.portfolio_return_calculator(positions, returns)
    plt.plot(prices['DateTime'], port_return.cumsum())
    plt.xticks(rotation=45)
    plt.ylabel('Cumulative return')
    plt.title("Cointegration of {} vs. {}.".format(tickers[0], tickers[1]))
    plt.show()

    sharpe_tr = utils.sharpe(port_return[:train_idx], 252)
    sharpe_test = utils.sharpe(port_return[train_idx:], 252)
    print("Sharpe ratio on the train {:.2f} and test {:.2f} sets respectively.".format(sharpe_tr, sharpe_test))

    cost_per_transaction = 0.0005
    port_return_minus_costs = port_return - utils.simple_transaction_costs(positions, cost_per_transaction)
    sharp_cost_adj_tr = utils.sharpe(port_return_minus_costs[:train_idx], 252)
    sharp_cost_adj_test = utils.sharpe(port_return_minus_costs[train_idx:], 252)
    print("Sharpe ratio on the train {:.2f} and test {:.2f} sets respectively "
          "after adjusting for transaction costs.".format(sharp_cost_adj_tr, sharp_cost_adj_test))





