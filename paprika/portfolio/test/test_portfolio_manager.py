from paprika.execution.order import Side, MarketOrder
from paprika.portfolio.order_manager import SimpleOrderManager
from paprika.portfolio.order_manager import TransactionCost, TransactionCostType
from paprika.portfolio.portfolio_manager import PortfolioManager
from paprika.portfolio.portfolio import Portfolio
from paprika.signal.signal_data import SignalData
# from paprika.portfolio.optimization import PortfolioOptimizer
# from paprika.portfolio.risk_policy import RiskPolicy

import pandas as pd


def test_portfolio_manager():
    # order = MarketOrder("MTA.FGBL201812", 20, Side.BUY, pd.to_datetime('2018-09-20 11:00').to_pydatetime())
    # tc = TransactionCost(TransactionCostType.FIXED, 0.01)
    # order_manager = SimpleOrderManager(tc)
    # fills, remaining_order = order_manager.accept_order(order, None, None)

    # orders = list(order)
    portfolio = Portfolio()
    signal_data = SingalData()
    portfolio_manager = PortfolioManager(order_manager)

    portfolio_manager.set_portfolio(portfolio)
    portfolio_manager.execute_signals(signal_data)


