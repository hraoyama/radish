from paprika.execution.order import Side, MarketOrder
from paprika.portfolio.order_manager import SimpleOrderManager
from paprika.portfolio.order_manager import TransactionCost, TransactionCostType
from paprika.portfolio.portfolio import Portfolio
from paprika.data.fetcher import HistoricalDataFetcher
import pandas as pd
from datetime import timedelta


def test_order_manager():
    base_currency = 'EUR'
    # balance = {base_currency: 10000000}
    # portfolio = Portfolio("Total", base_currency, balance)

    symbol = 'EUX.FESX201906'
    data_fetcher = HistoricalDataFetcher()
    df = data_fetcher.fetch_orderbook(symbol)
    timestamp = df.index[10]

    order = MarketOrder("EUX.FESX201906", 100, Side.BUY, timestamp)
    tc = TransactionCost(TransactionCostType.FIXED, 0.01)
    order_manager = SimpleOrderManager(tc)

    balance = {base_currency: 10000000}
    p1 = Portfolio("Total", base_currency, balance)
    print(p1)
    p1, remaining_order = order_manager.accept_order(p1, order, None, None)
    print(p1)
    order = MarketOrder("EUX.FESX201906", 100, Side.SELL, timestamp + timedelta(minutes=10))
    p1, remaining_order = order_manager.accept_order(p1, order, None, None)
    print(p1)
    print(p1.portfolio_records)

    balance = {base_currency: 10000}
    p2 = Portfolio("Total", base_currency, balance)
    print(p2)
    order = MarketOrder("EUX.FESX201906", 100, Side.BUY, timestamp)
    p2, remaining_order = order_manager.accept_order(p2, order, None, None)
    print(p2)
    order = MarketOrder("EUX.FESX201906", 100, Side.SELL, timestamp + timedelta(minutes=10))
    p2, remaining_order = order_manager.accept_order(p2, order, None, None)
    print(p2)
    print(p2.portfolio_records)

    assert remaining_order is None
    
    # fills, remaining_order = order_manager.accept_order(order, None, None)
