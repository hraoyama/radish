from paprika.execution.order import Side, MarketOrder
from paprika.portfolio.order_manager import SimpleOrderManager
from paprika.portfolio.order_manager import TransactionCost, TransactionCostType
from paprika.portfolio.portfolio_manager import PortfolioManager
from paprika.portfolio.portfolio import Portfolio
# from paprika.signal.signal_data import SignalData
# from paprika.portfolio.optimization import PortfolioOptimizer
# from paprika.portfolio.risk_policy import RiskPolicy
from paprika.data.fetcher import HistoricalDataFetcher
from paprika.data.data_type import DataType

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

    portfolio_manager = PortfolioManager(order_manager)

    symbol = 'EUX.FESX201906'
    df = data_fetcher.fetch_orderbook(symbol)
    timestamp = df.index[10]

    portfolio_manager.set_portfolio(portfolio, timestamp)

    orders[symbol] = MarketOrder(symbol, 10, Side.BUY, timestamp + timedelta(minutes=10))
    portfolio_manager.executing_orders_at_one_timestamp(orders)

    orders[symbol] = MarketOrder(symbol, 5, Side.SELL, timestamp + timedelta(minutes=20))
    portfolio_manager.executing_orders_at_one_timestamp(orders)
    print(portfolio_manager.portfolio)
    print(portfolio_manager.trades)
    print(portfolio_manager.portfolio_records)

  # signal_data = SingalData()
    # portfolio_manager.execute_signals(signal_data)



