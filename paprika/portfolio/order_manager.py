from paprika.execution.order import Order, OrderStatus, OrderType, Side, ExecutionResult
from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType

from datetime import datetime, timedelta
from abc import ABC
from aenum import Enum


class TransactionCostType(Enum):
    FIXED = 0
    SPREAD_RATIO = 1


class TransactionCost(object):
    def __init__(self, transaction_type: TransactionCostType, value):
        self._value = value
        self._type = transaction_type


class OrderManager(ABC):
    # queues
    # transaction cost
    # impact derivation
    # time delay
    
    def __init__(self):
        pass
    
    def accept_order(self, order, src, target):
        pass


class SimpleOrderManager(OrderManager):
    def __init__(self, transaction_cost: TransactionCost, time_delay=timedelta(microseconds=20)):
        self._cost = transaction_cost
        self._time_delay = time_delay
        pass
    
    def accept_order(self, order: Order, src, target):
        data = DataChannel.extract_time_span(DataChannel.name_to_data_type(order.symbol, DataType.ORDERBOOK),
                                             order.creation_time + self._time_delay)
        if data.shape[0] <= 0:
            # no data after this time, so nothing happens
            return None
        remaining = 0
        for idx_to_check in data.index:
            fills, remaining = self._extract_from_side(order, data.loc[idx_to_check])
            if remaining is None:
                break
        return None
    
    def _extract_from_side(self, order, book_data):
        executed_fills = []
        for level_num in range(5):
            executed_fills, remaining_order = self._fill_from_level(executed_fills, order, book_data, level=level_num)
            if not remaining_order:
                break
        
        if remaining_order:
            # assume a fill at 3x times the average fill price on the remaining
            # ExecutionResult(order_id=order.id, status=OrderStatus.FILLED, amount=order.amount, remaining=None,
            #                 avg_price=available_px, symbol=order.symbol, order_type=OrderType.MARKET,
            #                 side=order.side, price=available_px)
            pass
        else:
            pass
    
    def _fill_from_level(self, executed_fills, order, book_data, level=0):
        qty_lev_name = f'{str(order.side)}_Qty_Lev_{str(level)}'
        px_lev_name = f'{str(order.side)}_Px_Lev_{str(level)}'
        available_qty = book_data[[qty_lev_name]][0]
        available_px = book_data[[px_lev_name]][0]
        if available_qty >= order.amount:
            executed_fills.append(
                ExecutionResult(order_id=order.id, status=OrderStatus.FILLED, amount=order.amount, remaining=None,
                                avg_price=available_px, symbol=order.symbol, order_type=OrderType.MARKET,
                                side=order.side, price=available_px))
            return executed_fills, None
        else:
            order.amount = order.amount - available_qty
            executed_fills.append(
                ExecutionResult(order_id=order.id, status=OrderStatus.OPEN, amount=available_qty,
                                remaining=order.amount, avg_price=available_px, symbol=order.symbol,
                                order_type=OrderType.MARKET, side=order.side, price=available_px))
            return executed_fills, order
