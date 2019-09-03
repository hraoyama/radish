from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.signals.gold_cointegration import GoldSpread
from paprika.utils import utils
import matplotlib.pyplot as plt


def test_gold_cointegration():

    tckr1, tckr2 = 'GLD', 'GDX'
    gold_feed = Feed('GOLD_FEED', datetime(1950, 7, 1), datetime(2050, 1, 1))
    gold_feed.set_feed("GOLD_GDX", DataType.EOD_PRICE)

    gold_signal = GoldSpread(BETA=1.631, MEAN=0.052196, STD=1.9487, ENTRY=1, EXIT=0.5)
    gold_feed.add_subscriber(gold_signal)

    gold_signal.run()
    print(gold_signal.positions.head())
    positions = gold_signal.positions[['GLD', 'GDX']].fillna(method='ffill').values
    prices = gold_signal.prices

    train_idx = 252  # window where above parameters were estimated

    returns = utils.returns_calculator(prices[['GLD', 'GDX']], 1)
    port_return = utils.portfolio_return_calculator(positions, returns)
    plt.plot(prices['DateTime'], port_return.cumsum())
    plt.xticks(rotation=45)
    plt.ylabel('Cumulative return')
    plt.title("Cointegration of {} vs. {}.".format(tckr1, tckr2))
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