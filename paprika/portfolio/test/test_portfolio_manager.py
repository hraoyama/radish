from paprika.execution.order import Side, MarketOrder
from paprika.portfolio.order_manager import SimpleOrderManager
from paprika.portfolio.order_manager import TransactionCost, TransactionCostType
from paprika.portfolio.portfolio_manager import PortfolioManager
from paprika.portfolio.portfolio import Portfolio
# from paprika.portfolio.optimization import PortfolioOptimizer
from paprika.portfolio.risk_policy import RiskPolicy
from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.signals.signals.signal_bollinger_bands import BollingerBands
from paprika.signals.signal_cointegration import CointegrationSpread

import pandas as pd
from datetime import datetime, timedelta


def test_portfolio_manager():
    orders = {}

    tc = TransactionCost(TransactionCostType.FIXED, 0.01)
    order_manager = SimpleOrderManager(tc)

    base_currency = 'EUR'
    balance = {base_currency: 100000}
    portfolio = Portfolio('T', base_currency, balance)

    risk_policy = RiskPolicy()
    portfolio_manager = PortfolioManager(order_manager, risk_policy=risk_policy)

    portfolio_manager.portfolio = portfolio

    # symbol_patterns = ['EUX.FB.*']
    # df = DataChannel.fetch(symbol_patterns, data_type=DataType.ORDERBOOK)
    # symbols = df.index.get_level_values('Symbol').unique()
    # max_symbol = symbols[0]
    # max_row = df.loc[max_symbol].shape[0]
    # for symbol in symbols:
    #     if df.loc[symbol].shape[0] > max_row:
    #         max_symbol = symbol
    # symbol = max_symbol.replace(f'.{DataType.ORDERBOOK}', '')
    # df = DataChannel.fetch([symbol], data_type=DataType.ORDERBOOK)
    # timestamp = df.index.get_level_values('Date')[1000]
    # orders[symbol] = MarketOrder(symbol, 10, Side.BUY, timestamp + timedelta(minutes=10))
    # portfolio_manager.executing_orders_at_one_timestamp(orders)
    #
    # orders[symbol] = MarketOrder(symbol, 5, Side.SELL, timestamp + timedelta(minutes=20))
    # portfolio_manager.executing_orders_at_one_timestamp(orders)
    # print(portfolio_manager.portfolio)
    # print(portfolio_manager.trades)
    # print(portfolio_manager.portfolio_records)

    symbol_patterns = ['SP500.A.*']
    symbols = DataChannel.check_register(symbol_patterns)
    # df = DataChannel.fetch(symbol_patterns, data_type=DataType.CANDLE)
    # symbols = df.index.get_level_values('Symbol').unique()
    # symbols = [symbol.replace('.1D.Candle', '') for symbol in symbols]
    tickers = [symbols['mdb'][0], symbols['mdb'][1]]
    bollinger_feed = Feed('GOLD_FEED', datetime(2014, 7, 1), datetime(2014, 12, 1))
    bollinger_feed.set_feed(tickers, DataType.CANDLE, how='inner')
    gold_bollinger = BollingerBands(LOOKBACK=20, Y_NAME=tickers[0], X_NAME=tickers[1])
    bollinger_feed.add_subscriber(gold_bollinger)
    gold_bollinger.run()
    gold_signal_data = gold_bollinger.signal_data()
    signal_data = {'G': gold_signal_data}

    portfolio_manager.executing_signals(signal_data)
    print(portfolio_manager.portfolio.total_portfolio_records)

    print('ok')



