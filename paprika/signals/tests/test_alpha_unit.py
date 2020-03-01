from paprika.data.data_processor import DataProcessor
from paprika.data.data_type import DataType
from paprika.core.function_utils import add_return_to_dict_or_pandas_col_decorator
from paprika.data.processor_utils import get_return_series
from paprika.data.processor_utils import merge_data_frames_in_dict_values
from paprika.execution.order import Side, MarketOrder
from paprika.portfolio.order_manager import SimpleOrderManager
from paprika.portfolio.order_manager import TransactionCost, TransactionCostType
from paprika.portfolio.portfolio_manager import PortfolioManager
from paprika.portfolio.portfolio import Portfolio
from paprika.portfolio.risk_policy import RiskPolicy
from paprika.data.feed import Feed
from paprika.signals.signals.alpha_unit import AlphaUnit
from paprika.portfolio.analysis import PortfolioAnalyser
from paprika.data.data_channel import DataChannel
import numpy as np
import pandas as pd
from datetime import datetime


def test_alloc_Kelly():
    from paprika.alpha.alpha.alpha_gt import alpha_gt_1

    tickers = ['SP500.AAP.*']
    feed = Feed('SP500', datetime(2014, 1, 1), datetime(2015, 1, 1))
    feed.set_feed(tickers, DataType.CANDLE, how='inner')
    signal = AlphaUnit(alpha=alpha_gt_1)
    feed.add_subscriber(signal)
    signal.run()
    signal_data = signal.signal_data()
    signals_data = {'C': signal_data}
    tc = TransactionCost(TransactionCostType.FIXED, 0.01)
    order_manager = SimpleOrderManager(tc)
    base_currency = 'EUR'
    balance = {base_currency: 100000}
    portfolio = Portfolio('T', base_currency, balance)
    risk_policy = RiskPolicy()
    portfolio_manager = PortfolioManager(order_manager, risk_policy=risk_policy)
    portfolio_manager.portfolio = portfolio
    portfolio = portfolio_manager.executing_signals(signals_data)

    print(portfolio.total_portfolio_records)

    result = PortfolioAnalyser.analyse(portfolio_manager.portfolio)
    print(result)