from paprika.execution.order import Side, MarketOrder
from paprika.portfolio.order_manager import SimpleOrderManager
from paprika.portfolio.order_manager import TransactionCost, TransactionCostType
from paprika.portfolio.portfolio_manager import PortfolioManager
from paprika.portfolio.portfolio import Portfolio
# from paprika.portfolio.optimization import PortfolioOptimizer
from paprika.portfolio.risk_policy import RiskPolicy
from paprika.data.fetcher import HistoricalDataFetcher
from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.signals.signal_cointegration import CointegrationSpread

import pandas as pd
from datetime import datetime, timedelta


def test_portfolio_manager():
    orders = {}
    data_fetcher = HistoricalDataFetcher()

    tc = TransactionCost(TransactionCostType.FIXED, 0.01)
    order_manager = SimpleOrderManager(tc)

    base_currency = 'EUR'
    balance = {base_currency: 100000}
    portfolio = Portfolio('T', base_currency, balance)

    risk_policy = RiskPolicy()
    portfolio_manager = PortfolioManager(order_manager, risk_policy=risk_policy)

    symbol = 'EUX.FESX201906'
    df = data_fetcher.fetch_orderbook(symbol)
    timestamp = df.index[10]

    portfolio_manager.portfolio = portfolio

    orders[symbol] = MarketOrder(symbol, 10, Side.BUY, timestamp + timedelta(minutes=10))
    portfolio_manager.executing_orders_at_one_timestamp(orders)

    orders[symbol] = MarketOrder(symbol, 5, Side.SELL, timestamp + timedelta(minutes=20))
    portfolio_manager.executing_orders_at_one_timestamp(orders)
    print(portfolio_manager.portfolio)
    print(portfolio_manager.trades)
    print(portfolio_manager.portfolio_records)

    tickers = ["GLD", "GDX"]
    gold_feed = Feed('GOLD_FEED', datetime(1990, 7, 1), datetime(2010, 1, 1))
    gold_feed.set_feed(tickers, DataType.OHLCVAC_PRICE, how='inner')
    gold_signal = CointegrationSpread(BETA=1.631, MEAN=0.052196, STD=1.9487, ENTRY=1, EXIT=0.5,
                                      Y_NAME="GLD", X_NAME="GDX")
    gold_feed.add_subscriber(gold_signal)
    gold_signal.run()
    gold_signal_data = gold_signal.signal_data()
    signal_data = {'G': gold_signal_data}
    portfolio_manager.executing_signals(signal_data)



