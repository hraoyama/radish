from paprika.execution.order import Side, MarketOrder
from paprika.portfolio.order_manager import SimpleOrderManager
from paprika.portfolio.order_manager import TransactionCost, TransactionCostType
from paprika.portfolio.portfolio import Portfolio
# from paprika.data.fetcher import HistoricalDataFetcher
from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType

import pandas as pd
from datetime import timedelta
from absl import logging


def test_order_manager():
    # create a portolio
    base_currency = 'EUR'
    balance = {base_currency: 10000000}
    portfolio = Portfolio("Total", base_currency, balance)

    tc = TransactionCost(TransactionCostType.FIXED, 0.01)
    order_manager = SimpleOrderManager(tc)

    # find the symbol with most rows in orderbook
    symbol_patterns = ['EUX.FB.*']
    df = DataChannel.fetch(symbol_patterns, data_type=DataType.ORDERBOOK)
    symbols = df.index.get_level_values('Symbol').unique()
    max_symbol = symbols[0]
    max_row = df.loc[max_symbol].shape[0]
    for symbol in symbols:
        if df.loc[symbol].shape[0] > max_row:
            max_symbol = symbol
    symbol = max_symbol.replace(f'.{DataType.ORDERBOOK}', '')
    df = DataChannel.fetch([symbol], data_type=DataType.ORDERBOOK)
    timestamp = df.index.get_level_values('Date')[1000]

    # use this symbol to create a order
    order = MarketOrder(symbol, 100, Side.BUY, timestamp)
    print(portfolio)
    portfolio, remaining_order = order_manager.accept_order(portfolio, order, None, None)
    print(portfolio)

    # second order
    order = MarketOrder(symbol, 100, Side.SELL, timestamp + timedelta(minutes=10))
    portfolio, remaining_order = order_manager.accept_order(portfolio, order, None, None)
    print(portfolio)
    print(portfolio.portfolio_records)

    symbol_patterns = ['ETF.XS.*']
    df = DataChannel.fetch(symbol_patterns, data_type=DataType.TRADES)
    symbols = df.index.get_level_values('Symbol').unique()
    max_symbol = symbols[0]
    max_row = df.loc[max_symbol].shape[0]
    for symbol in symbols:
        if df.loc[symbol].shape[0] > max_row:
            max_symbol = symbol
    symbol = max_symbol.replace(f'.{DataType.TRADES}', '')
    df = DataChannel.fetch([symbol], data_type=DataType.TRADES)
    timestamp = df.index.get_level_values('Date')[1000]

    order = MarketOrder(symbol, 100, Side.BUY, timestamp)
    portfolio, remaining_order = order_manager.accept_order(portfolio, order)
    order = MarketOrder(symbol, 100, Side.SELL, timestamp)
    portfolio, remaining_order = order_manager.accept_order(portfolio, order)
    print(portfolio)
    print(portfolio.portfolio_records)

    symbol_patterns = ['SP500.A.*']
    df = DataChannel.fetch(symbol_patterns, data_type=DataType.CANDLE)
    symbols = df.index.get_level_values('Symbol').unique()
    symbol = symbols[0]
    timestamp = df.index.get_level_values('Date')[1000]
    symbol = symbol.replace(f'.1D.{DataType.CANDLE}', '')

    order = MarketOrder(symbol, 100, Side.BUY, timestamp)
    portfolio, remaining_order = order_manager.accept_order(portfolio, order)
    order = MarketOrder(symbol, 100, Side.SELL, timestamp)
    portfolio, remaining_order = order_manager.accept_order(portfolio, order)
    print(portfolio)
    print(portfolio.portfolio_records)

    assert remaining_order is None
    
    # fills, remaining_order = order_manager.accept_order(order, None, None)
