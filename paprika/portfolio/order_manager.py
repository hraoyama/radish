from paprika.execution.order import Order, OrderStatus, OrderType, Side, ExecutionResult
from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.portfolio.portfolio import Portfolio
from paprika.data.constants import TradeColumnName, OrderBookColumnName, CandleColumnName

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
                     src: Optional[str] = None,
                     target: Optional[str] = None):
        pass


class SimpleOrderManager(OrderManager):
    """
    Only spot trading. No short.
    """
    def __init__(self,
                 transaction_cost: TransactionCost,
                 time_delay: Optional[timedelta] = timedelta(microseconds=20),
                 orderbook_time_span: Optional[timedelta] = timedelta(seconds=60),
                 trade_time_span: Optional[timedelta] = timedelta(hours=12),
                 candle_frequency: Optional[str] = '1D',
                 factor_if_exhausted: Optional[int] = 3.0):
        super().__init__()
        self._cost = transaction_cost
        self._time_delay = time_delay
        self._orderbook_time_span = orderbook_time_span
        self._trade_time_span = trade_time_span
        self._candle_time_span = pd.to_timedelta(candle_frequency)
        self._factor_if_exhausted = factor_if_exhausted

    def accept_order(self,
                     portfolio: Portfolio,
                     order: Order,
                     src: Optional[str] = None,
                     target: Optional[str] = None
                     ):
        # try orderbook data at first
        data = DataChannel.fetch([order.symbol],
                                 DataType.ORDERBOOK,
                                 start=order.creation_time + self._time_delay,
                                 end=order.creation_time + self._orderbook_time_span)

        # data2 = DataChannel.extract_time_span(DataChannel.name_to_data_type(order.symbol,
        #                                                                    DataType.ORDERBOOK),
        #                                      order.creation_time + self._time_delay)

        if data is not None:
            data = data.unstack('Symbol')
            remaining_order = copy.copy(order)
            for idx_to_check in data.index:
                portfolio, remaining_order = self._extract_from_side(portfolio,
                                                                     remaining_order,
                                                                     data.loc[idx_to_check])
                portfolio.add_portfolio_records(idx_to_check)
                if remaining_order.amount == 0:
                    remaining_order = None
                    break
        else:
            # then try trade data
            data = DataChannel.fetch([order.symbol],
                                     DataType.TRADES,
                                     start=order.creation_time + self._time_delay,
                                     end=order.creation_time + self._trade_time_span)
            if data is not None:
                data = data.unstack('Symbol')
                remaining_order = copy.copy(order)
                for idx_to_check in data.index:
                    portfolio, remaining_order = self._extract_from_trade(portfolio,
                                                                          remaining_order,
                                                                          data.loc[idx_to_check])
                    portfolio.add_portfolio_records(idx_to_check)
                    if remaining_order.amount == 0:
                        remaining_order = None
                        break
            else:
                data = DataChannel.fetch([order.symbol],
                                         DataType.CANDLE,
                                         start=order.creation_time + self._time_delay,
                                         end=order.creation_time + self._candle_time_span)
                if data is not None:
                    data = data.unstack('Symbol')
                    remaining_order = copy.copy(order)
                    for idx_to_check in data.index:
                        portfolio, remaining_order = self._extract_from_candle(portfolio,
                                                                               remaining_order,
                                                                               data.loc[idx_to_check])
                        portfolio.add_portfolio_records(idx_to_check)
                        if remaining_order.amount == 0:
                            remaining_order = None
                            break

                # no data after this time, so nothing happens
                return portfolio, None

        # fills = []

        # inform target with the transaction costs

        return portfolio, remaining_order

    def _extract_from_candle(self,
                             portfolio: Portfolio,
                             order: Order,
                             trade_data: pd.DataFrame
                             ):
        available_px = trade_data[CandleColumnName.High].values[0]
        available_qty = trade_data[CandleColumnName.Volume].values[0]
        executed_fill, remaining_order = self._fill_logic(order,
                                                          portfolio,
                                                          available_qty,
                                                          available_px)
        portfolio.record_trades([executed_fill])
        return portfolio, remaining_order

    def _extract_from_trade(self,
                            portfolio: Portfolio,
                            order: Order,
                            trade_data: pd.DataFrame
                            ):
        available_px = trade_data[TradeColumnName.Price].values[0]
        available_qty = trade_data[TradeColumnName.Qty].values[0]
        executed_fill, remaining_order = self._fill_logic(order,
                                                          portfolio,
                                                          available_qty,
                                                          available_px)
        portfolio.record_trades([executed_fill])
        return portfolio, remaining_order

    def _extract_from_side(self,
                           portfolio: Portfolio,
                           order: Order,
                           book_data: pd.DataFrame):
        # executed_fills = []
        remaining_order = order
        for level_num in range(5):
            executed_fill, remaining_order = self._fill_from_level(portfolio,
                                                                   remaining_order,
                                                                   book_data,
                                                                   level=level_num)
            # executed_fills.append(executed_fill)
            portfolio.record_trades([executed_fill])
            if remaining_order.amount == 0:
                break

        return portfolio, remaining_order

    def _fill_from_level(self,
                         portfolio: Portfolio,
                         order: Order,
                         book_data: pd.DataFrame,
                         level: Optional[int] = 0):
        qty_lev_name = f'{str(order.side)}_Qty_Lev_{str(level)}'
        px_lev_name = f'{str(order.side)}_Px_Lev_{str(level)}'
        available_qty = book_data[[qty_lev_name]][0]
        available_px = book_data[[px_lev_name]][0]
        executed_fill, remaining_order = self._fill_logic(order,
                                                          portfolio,
                                                          available_qty,
                                                          available_px)
        return executed_fill, remaining_order

    def _fill_logic(self,
                    order,
                    portfolio,
                    available_qty,
                    available_px):
        if available_qty >= order.amount:
            if order.side == Side.BUY:
                costs = (self._cost.value + 1) * order.amount * available_px
                fund = portfolio.balance[portfolio.base_currency]
                if fund < costs:
                    amount = int(fund / ((self._cost.value + 1) * available_px))
                    costs = (self._cost.value + 1) * amount * available_px
                else:
                    amount = order.amount
            elif order.side == Side.SELL:
                asset_amount = portfolio.balance[order.symbol]
                if asset_amount < order.amount:
                    costs = (self._cost.value - 1) * asset_amount * available_px
                    amount = asset_amount
                else:
                    costs = (self._cost.value - 1) * order.amount * available_px
                    amount = order.amount
            else:
                raise NotImplementedError(f'Not support {order.side} now.')

            order.amount = 0
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


class OrderManager(OrderManager):
    """
    Allow short.
    Has not taken leverage.
    """
    def __init__(self,
                 transaction_cost: TransactionCost,
                 time_delay: Optional[timedelta] = timedelta(microseconds=20),
                 orderbook_time_span: Optional[timedelta] = timedelta(seconds=60),
                 trade_time_span: Optional[timedelta] = timedelta(hours=12),
                 candle_frequency: Optional[str] = '1D',
                 factor_if_exhausted: Optional[int] = 3.0):
        super().__init__()
        self._cost = transaction_cost
        self._time_delay = time_delay
        self._orderbook_time_span = orderbook_time_span
        self._trade_time_span = trade_time_span
        self._candle_time_span = pd.to_timedelta(candle_frequency)
        self._factor_if_exhausted = factor_if_exhausted

    def accept_order(self,
                     portfolio: Portfolio,
                     order: Order,
                     src: Optional[str] = None,
                     target: Optional[str] = None
                     ):
        # try orderbook data at first
        data = DataChannel.fetch([order.symbol],
                                 DataType.ORDERBOOK,
                                 start=order.creation_time + self._time_delay,
                                 end=order.creation_time + self._orderbook_time_span)

        if data is not None:
            data = data.unstack('Symbol')
            remaining_order = copy.copy(order)
            for idx_to_check in data.index:
                portfolio, remaining_order = self._extract_from_side(portfolio,
                                                                     remaining_order,
                                                                     data.loc[idx_to_check])
                portfolio.add_portfolio_records(idx_to_check)
                if remaining_order.amount == 0:
                    remaining_order = None
                    break
        else:
            # then try trade data
            data = DataChannel.fetch([order.symbol],
                                     DataType.TRADES,
                                     start=order.creation_time + self._time_delay,
                                     end=order.creation_time + self._trade_time_span)
            if data is not None:
                data = data.unstack('Symbol')
                remaining_order = copy.copy(order)
                for idx_to_check in data.index:
                    portfolio, remaining_order = self._extract_from_trade(portfolio,
                                                                          remaining_order,
                                                                          data.loc[idx_to_check])
                    portfolio.add_portfolio_records(idx_to_check)
                    if remaining_order.amount == 0:
                        remaining_order = None
                        break
            else:
                data = DataChannel.fetch([order.symbol],
                                         DataType.CANDLE,
                                         start=order.creation_time + self._time_delay,
                                         end=order.creation_time + self._candle_time_span)
                if data is not None:
                    data = data.unstack('Symbol')
                    remaining_order = copy.copy(order)
                    for idx_to_check in data.index:
                        portfolio, remaining_order = self._extract_from_candle(portfolio,
                                                                               remaining_order,
                                                                               data.loc[idx_to_check])
                        portfolio.add_portfolio_records(idx_to_check)
                        if remaining_order.amount == 0:
                            remaining_order = None
                            break

                # no data after this time, so nothing happens
                return portfolio, None

        return portfolio, remaining_order

    def _extract_from_candle(self,
                             portfolio: Portfolio,
                             order: Order,
                             trade_data: pd.DataFrame
                             ):
        available_px = trade_data[CandleColumnName.High].values[0]
        available_qty = trade_data[CandleColumnName.Volume].values[0]
        executed_fill, remaining_order = self._fill_logic(order,
                                                          portfolio,
                                                          available_qty,
                                                          available_px)
        portfolio.record_trades([executed_fill])
        return portfolio, remaining_order

    def _extract_from_trade(self,
                            portfolio: Portfolio,
                            order: Order,
                            trade_data: pd.DataFrame
                            ):
        available_px = trade_data[TradeColumnName.Price].values[0]
        available_qty = trade_data[TradeColumnName.Qty].values[0]
        executed_fill, remaining_order = self._fill_logic(order,
                                                          portfolio,
                                                          available_qty,
                                                          available_px)
        portfolio.record_trades([executed_fill])
        return portfolio, remaining_order

    def _extract_from_side(self,
                           portfolio: Portfolio,
                           order: Order,
                           book_data: pd.DataFrame):
        remaining_order = order
        for level_num in range(5):
            executed_fill, remaining_order = self._fill_from_level(portfolio,
                                                                   remaining_order,
                                                                   book_data,
                                                                   level=level_num)
            portfolio.record_trades([executed_fill])
            if remaining_order.amount == 0:
                break

        return portfolio, remaining_order

    def _fill_from_level(self,
                         portfolio: Portfolio,
                         order: Order,
                         book_data: pd.DataFrame,
                         level: Optional[int] = 0):
        qty_lev_name = f'{str(order.side)}_Qty_Lev_{str(level)}'
        px_lev_name = f'{str(order.side)}_Px_Lev_{str(level)}'
        available_qty = book_data[[qty_lev_name]][0]
        available_px = book_data[[px_lev_name]][0]
        executed_fill, remaining_order = self._fill_logic(order,
                                                          portfolio,
                                                          available_qty,
                                                          available_px)
        return executed_fill, remaining_order

    def _fill_logic(self,
                    order,
                    portfolio,
                    available_qty,
                    available_px):
        """
        This logic support short.
        """
        if available_qty >= order.amount:
            if order.side == Side.BUY:
                costs = (self._cost.value + 1) * order.amount * available_px
                fund = portfolio.balance[portfolio.base_currency]
                if fund < costs:
                    amount = int(fund / ((self._cost.value + 1) * available_px))
                    costs = (self._cost.value + 1) * amount * available_px
                else:
                    amount = order.amount
            elif order.side == Side.SELL:
                asset_amount = portfolio.balance[order.symbol]
                if asset_amount >= order.amount:
                    costs = (self._cost.value - 1) * order.amount * available_px
                    #     amount = order.amount
                    #     order.amount = 0
                costs = (self._cost.value + 1) * order.amount * available_px
                fund = portfolio.balance[portfolio.base_currency]
                if fund < costs:
                    amount = int(fund / ((self._cost.value + 1) * available_px))
                    costs = (self._cost.value + 1) * amount * available_px
                else:
                    amount = order.amount

                # asset_amount = portfolio.balance[order.symbol]
                # if asset_amount < order.amount:
                #     costs = (self._cost.value - 1) * asset_amount * available_px
                #     amount = asset_amount
                #     order.amount = 0
                # else:
                #     costs = (self._cost.value - 1) * order.amount * available_px
                #     amount = order.amount
                #     order.amount = order.amount - amount
            else:
                raise NotImplementedError(f'Not support {order.side} now.')
            order.amount = 0

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