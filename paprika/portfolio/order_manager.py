from paprika.execution.order import Order, OrderStatus, OrderType, Side, ExecutionResult
from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType
from paprika.data.fetcher import HistoricalDataFetcher
from paprika.portfolio.portfolio import Portfolio
from paprika.data.constants import OrderBookColumnName

from datetime import datetime, timedelta
from abc import ABC
from aenum import Enum
import pandas as pd
from typing import List, Optional
import copy

class TransactionCostType(Enum):
    FIXED = 0
    SPREAD_RATIO = 1


class TransactionCost(object):
    def __init__(self, transaction_type: TransactionCostType, value):
        self._value = value
        self._type = transaction_type

    @property
    def value(self):
        return self._value


class OrderManager(ABC):
    # queues
    # transaction cost
    # impact derivation
    # time delay

    def __init__(self):
        pass

    def accept_order(self,
                     portfolio: Portfolio,
                     order,
                     src,
                     target):
        pass


class SimpleOrderManager(OrderManager):
    def __init__(self, transaction_cost: TransactionCost, time_delay=timedelta(microseconds=20),
                 factor_if_exhausted=3.0):
        super().__init__()
        self._cost = transaction_cost
        self._time_delay = time_delay
        self._factor_if_exhausted = factor_if_exhausted
        self.fetcher = HistoricalDataFetcher()

    def accept_order(self,
                     portfolio: Portfolio,
                     order: Order,
                     src,
                     target):
        data = DataChannel.extract_time_span(DataChannel.name_to_data_type(order.symbol,
                                                                           DataType.ORDERBOOK),
                                             order.creation_time + self._time_delay)
        if data is None:
            # no data after this time, so nothing happens
            return None, None

        fills = []
        remaining_order = copy.copy(order)
        for idx_to_check in data.index:
            fills, remaining_order = self._extract_from_side(portfolio,
                                                             remaining_order,
                                                             data.loc[idx_to_check])
            portfolio.add_portfolio_records(idx_to_check)
            if remaining_order.amount == 0:
                remaining_order = None
                break

        # inform target with the transaction costs

        return fills, remaining_order

    def _extract_from_side(self,
                           portfolio: Portfolio,
                           order: Order,
                           book_data: pd.DataFrame):
        executed_fills = []
        remaining_order = order
        for level_num in range(5):
            executed_fill, remaining_order = self._fill_from_level(portfolio,
                                                                   remaining_order,
                                                                   book_data,
                                                                   level=level_num)
            executed_fills.append(executed_fill)
            portfolio.record_trades([executed_fill])
            if remaining_order.amount == 0:
                break

        # if remaining_order:
        #     # assume a fill at 3x times the average fill price on the remaining
        #     avg_px, avg_qty, vw_px, max_px, min_px = ExecutionResult.get_fill_info(executed_fills)
        #     remaining_fill_px = min_px + self._factor_if_exhausted * (vw_px - min_px)
        #     executed_fills.append(
        #         ExecutionResult(order_id=order.id, status=OrderStatus.FILLED, amount=order.amount, remaining=None,
        #                         avg_price=remaining_fill_px, symbol=order.symbol, order_type=OrderType.MARKET,
        #                         side=order.side, price=remaining_fill_px))
        #     remaining_order = None

        return executed_fills, remaining_order

    def _fill_from_level(self,
                         portfolio: Portfolio,
                         order: Order,
                         book_data: pd.DataFrame,
                         level: Optional[int] = 0):
        qty_lev_name = f'{str(order.side)}_Qty_Lev_{str(level)}'
        px_lev_name = f'{str(order.side)}_Px_Lev_{str(level)}'
        available_qty = book_data[[qty_lev_name]][0]
        available_px = book_data[[px_lev_name]][0]
        if available_qty >= order.amount:
            if order.side == Side.BUY:
                costs = (self._cost.value + 1) * order.amount * available_px
                fund = portfolio.balance[portfolio.base_currency]
                if fund < costs:
                    amount = int(fund / ((self._cost.value + 1) * available_px))
                    costs = (self._cost.value + 1) * amount * available_px
                    order.amount = 0
                else:
                    amount = order.amount
                    order.amount = order.amount - amount
            else:
                asset_amount = portfolio.balance[order.symbol]
                if asset_amount < order.amount:
                    costs = (self._cost.value - 1) * asset_amount * available_px
                    amount = asset_amount
                    order.amount = 0
                else:
                    costs = (self._cost.value - 1) * order.amount * available_px
                    amount = order.amount
                    order.amount = order.amount - amount

            executed_fill = ExecutionResult(order_id=order.id, status=OrderStatus.FILLED, amount=amount,
                                            remaining=order.amount, avg_price=available_px, symbol=order.symbol,
                                            order_type=OrderType.MARKET, side=order.side, price=available_px,
                                            costs=costs)
            return executed_fill, order
        else:
            if order.side == Side.BUY:
                fund = portfolio.balance[portfolio.base_currency]
                costs = (self._cost.value + 1) * available_qty * available_px
                if fund < costs:
                    amount = int(fund / ((self._cost.value + 1) * available_px))
                    costs = (self._cost.value + 1) * amount * available_px
                    order.amount = 0
                else:
                    amount = available_qty
                    order.amount = order.amount - amount
            else:
                asset_amount = portfolio.balance[order.symbol]
                if asset_amount < available_qty:
                    costs = (self._cost.value - 1) * asset_amount * available_px
                    amount = asset_amount
                    order.amount = 0
                else:
                    costs = (self._cost.value - 1) * available_qty * available_px
                    amount = available_qty
                    order.amount = order.amount - amount

            executed_fill = ExecutionResult(order_id=order.id, status=OrderStatus.OPEN, amount=amount,
                                            remaining=order.amount, avg_price=available_px, symbol=order.symbol,
                                            order_type=OrderType.MARKET, side=order.side, price=available_px,
                                            costs=costs)
            return executed_fill, order
