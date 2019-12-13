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
from paprika.portfolio.analysis import PortfolioAnalyser
import pandas as pd
from datetime import datetime, timedelta


def test_portfolio_manager():
    tc = TransactionCost(TransactionCostType.FIXED, 0.01)
    order_manager = SimpleOrderManager(tc)

    base_currency = 'EUR'
    balance = {base_currency: 100000}
    portfolio = Portfolio('T', base_currency, balance)

    risk_policy = RiskPolicy()
    portfolio_manager = PortfolioManager(order_manager, risk_policy=risk_policy)

    portfolio_manager.portfolio = portfolio

    symbol_patterns = ['SP500.A.*']
    symbols = DataChannel.check_register(symbol_patterns)
    tickers = [symbols['mdb'][0], symbols['mdb'][1]]
    bollinger_feed = Feed('GOLD_FEED', datetime(2014, 7, 1), datetime(2014, 11, 1))
    bollinger_feed.set_feed(tickers, DataType.CANDLE, how='inner')
    gold_bollinger = BollingerBands(LOOKBACK=20, Y_NAME=tickers[0], X_NAME=tickers[1])
    bollinger_feed.add_subscriber(gold_bollinger)
    gold_bollinger.run()
    gold_signal_data = gold_bollinger.signal_data()
    signal_data = {'G': gold_signal_data}

    portfolio_manager.executing_signals(signal_data)
    print(portfolio_manager.portfolio.total_portfolio_records)

    result = PortfolioAnalyser.analyse(portfolio_manager.portfolio)
    print(result)



