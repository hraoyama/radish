from paprika.execution.order import Side, MarketOrder
from paprika.portfolio.order_manager import SimpleOrderManager
from paprika.portfolio.order_manager import TransactionCost, TransactionCostType

import pandas as pd


def test_order_manager():
    order = MarketOrder("MTA.FGBL201812", 20, Side.Ask, pd.to_datetime('2018-09-20 11:00').to_pydatetime())
    
    tc = TransactionCost(TransactionCostType.FIXED, 0.01)
    order_manager = SimpleOrderManager(tc)
    
    order_manager.accept_order(order, None, None)
    # fills, remaining_order = order_manager.accept_order(order, None, None)
