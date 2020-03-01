from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.signals.signals.simple_cointegration import CointegrationSpread
from paprika.utils import utils
import pandas as pd


def test_simple_cointegration():

    tickers = ["GLD", "GDX"]
    gold_feed = Feed('GOLD_FEED', datetime(1950, 7, 1), datetime(2050, 1, 1))
    gold_feed.set_feed(tickers, DataType.OHLCVAC_PRICE, how='inner')
    gold_signal = CointegrationSpread(BETA=1.631, MEAN=0.052196, STD=1.9487, ENTRY=1, EXIT=0.5, Y_NAME="GLD", X_NAME="GDX")
    gold_feed.add_subscriber(gold_signal)

    gold_signal.run()
    gold_gdx_positions = gold_signal.positions
    gold_gdx_prices = gold_signal.prices
    column_names = [gold_signal.y_name, gold_signal.x_name]
    gold_gdx_returns = utils.returns_calculator(gold_gdx_prices[column_names], 1)
    # daily P&L of the strategy
    gold_gdx_pnl = utils.portfolio_return_calculator(gold_gdx_positions[column_names], gold_gdx_returns)
    _ = utils.stats_print(gold_gdx_prices['DateTime'], pd.Series(gold_gdx_pnl))

    train_idx = 252
    sharpe_tr = utils.sharpe(gold_gdx_pnl[:train_idx], 252)
    sharpe_test = utils.sharpe(gold_gdx_pnl[train_idx:], 252)
    print("Sharpe ratio on the train {:.2f} and test {:.2f} sets respectively.".format(sharpe_tr, sharpe_test))

    cost_per_transaction = 0.0005
    port_return_minus_costs = gold_gdx_pnl - utils.simple_transaction_costs(gold_gdx_positions[column_names],
                                                                            cost_per_transaction)
    sharp_cost_adj_tr = utils.sharpe(port_return_minus_costs[:train_idx], 252)
    sharp_cost_adj_test = utils.sharpe(port_return_minus_costs[train_idx:], 252)
    print("Sharpe ratio on the train {:.2f} and test {:.2f} sets respectively "
          "after adjusting for transaction costs.".format(sharp_cost_adj_tr, sharp_cost_adj_test))
