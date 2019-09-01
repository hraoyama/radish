from enum import Enum
from typing import NamedTuple
import aenum

import numpy as np

from paprika.utils.types import float_type

_id_generator = int(0)


class OrderType(Enum):
    MARKET = 0,
    LIMIT = 1


class Side(aenum.Enum):
    BUY = 0
    SELL = 1
    
    def __str__(self):
        if self.value == 0:
            return 'Ask'
        elif self.value == 1:
            return 'Bid'
        else:
            return self.name


class TimeInForce(Enum):
    # Good Till Cancel
    GTC = 0,
    # Immediate or Cancel
    IOC = 1,
    # Fill or Kill
    FOK = 2


class OrderStatus(Enum):
    FILLED = 0,
    OPEN = 1,
    REJECTED = 2


class Order:
    def __init__(self, symbol, amount, side, creation_time):
        """Negative amount means sell"""
        global _id_generator
        self.id = _id_generator
        _id_generator += 1
        self.symbol = symbol
        self.amount = float_type()(amount)
        self.creation_time = creation_time
        self.side = side


class MarketOrder(Order):
    def __init__(self, symbol, amount, side, creation_time):
        super().__init__(symbol, amount, side, creation_time)
    
    def __repr__(self):
        return f'MarketOrder(id={self.id}, symbol={self.symbol}, amount={self.amount}, creation_time={self.creation_time})'


class LimitOrder(Order):
    def __init__(self,
                 symbol,
                 amount,
                 side,
                 price,
                 creation_time,
                 time_in_force=TimeInForce.GTC,
                 post_only=False):
        super().__init__(symbol, amount, side, creation_time)
        self.limit_price = float_type()(price)
        self.time_in_force = time_in_force
        self.post_only = post_only
    
    def __repr__(self):
        return f'LimitOrder(id={self.id}, symbol={self.symbol}, amount={self.amount}, creation_time={self.creation_time})'


# class ExecutionResult(object):
#     def __init__(self,
#                  succeed=None,
#                  order_id=None,
#                  avg_price=None,
#                  quantity_filled=None,
#                  quantity_remaining=None,
#                  side=None):
#         self.succeed = succeed
#         self.order_id = order_id
#         self.avg_price = avg_price
#         self.quantity_filled = quantity_filled
#         self.quantity_remaining = quantity_remaining
#         self.side = side
#
#     @staticmethod
#     def failed_result():
#         return ExecutionResult(succeed=False)
#
#     def __str__(self):
#         return str({
#             'succeed': self.succeed,
#             'order_id': self.order_id,
#             'avg_price': self.avg_price,
#             'quantity_filled': self.quantity_filled,
#             'quantity_remaining': self.quantity_remaining
#         })


class ExecutionResult(NamedTuple):
    @staticmethod
    def get_fill_info(list_of_executions):
        pxs = [execution_result.avg_price for execution_result in list_of_executions]
        qtys = [execution_result.amount for execution_result in list_of_executions]
        return np.mean(pxs), np.mean(qtys), np.sum([px * qty for px, qty in zip(pxs, qtys)]) / np.sum(qtys), \
               np.max(pxs), np.min(pxs)
    
    order_id: str
    status: OrderStatus
    amount: float
    remaining: float
    avg_price: float
    symbol: str = None
    order_type: OrderType = None
    side: str = None
    price: float = None
    costs: float = None
