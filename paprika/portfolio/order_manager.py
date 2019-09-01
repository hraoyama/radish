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
    def __init__(self, transaction_cost: TransactionCost, time_delay=timedelta(microseconds=20),
                 factor_if_exhausted=3.0):
        self._cost = transaction_cost
        self._time_delay = time_delay
        self._factor_if_exhausted = factor_if_exhausted
        pass
    
    def accept_order(self, order: Order, src, target):
        data = DataChannel.extract_time_span(DataChannel.name_to_data_type(order.symbol, DataType.ORDERBOOK),
                                             order.creation_time + self._time_delay)
        if data is None:
            # no data after this time, so nothing happens
            return None
        
        for idx_to_check in data.index:
            fills, remaining = self._extract_from_side(order, data.loc[idx_to_check])
            if remaining is None:
                break
        
        # inform target with the transaction costs
        
        return fills, remaining
    
    def _extract_from_side(self, order, book_data):
        executed_fills = []
        for level_num in range(5):
            executed_fills, remaining_order = self._fill_from_level(executed_fills, order, book_data, level=level_num)
            if not remaining_order:
                break
        
        if remaining_order:
            # assume a fill at 3x times the average fill price on the remaining
            avg_px, avg_qty, vw_px, max_px, min_px = ExecutionResult.get_fill_info(executed_fills)
            remaining_fill_px = min_px + self._factor_if_exhausted * (vw_px - min_px)
            executed_fills.append(
                ExecutionResult(order_id=order.id, status=OrderStatus.FILLED, amount=order.amount, remaining=None,
                                avg_price=remaining_fill_px, symbol=order.symbol, order_type=OrderType.MARKET,
                                side=order.side, price=remaining_fill_px))
            remaining_order = None
        
        return executed_fills, remaining_order
    
    @staticmethod
    def _fill_from_level(executed_fills, order, book_data, level=0):
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
