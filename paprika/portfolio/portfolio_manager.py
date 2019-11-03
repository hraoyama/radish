import copy
from bisect import bisect_left
from collections import defaultdict
from numbers import Real
from typing import Dict, List, Mapping, NamedTuple, Tuple, Optional
import pandas as pd
from datetime import datetime
from absl import logging

from paprika.core.api import get_current_frame_timestamp
from paprika.core.context import get_context
from paprika.portfolio.account_type import AccountType
from paprika.portfolio.portfolio import (FuturePortfolio, MarginPortfolio,
                                         Portfolio)
from paprika.utils.distribution import Dist
from paprika.utils.types import float_type
from paprika.utils.utils import currency_pair, isclose
from paprika.portfolio.risk_policy import RiskPolicy
from paprika.portfolio.order_manager import OrderManager
from paprika.portfolio.optimization import PortfolioOptimizer
from paprika.execution.order import Side, Order, MarketOrder, ExecutionResult, OrderType, LimitOrder
from papriak.signal.signal_data import SignalData


class PortfolioManager:
    def __init__(self, order_manager: OrderManager,
                 optimizer: Optional[PortfolioOptimizer] = None,
                 risk_policy: Optional[RiskPolicy] = None):
        # if portfolio_by_source is None:
        #     portfolio_by_source = {}
        #
        # # (source, account_type) -> portfolio
        # self.portfolio_by_source: Dict[Tuple[str, AccountType], Portfolio] = \
        #     portfolio_by_source.copy()

        # self.trades = defaultdict(list)
        # self.received_orders = []
        self._remaining_orders = dict()
        self._executed_orders = []
        # self.trades_index = defaultdict(list)
        self.order_manager = order_manager

        self.optimizer = optimizer
        self.risk_policy = risk_policy
        self._portfolio = None

    def record_trade(self, fills: ExecutionResult):
        self._executed_orders.append(fills)

    def merge_signal_timestamps(self, signal_data: Dict[Str, SignalData]):
        dfs = [each_signal_data.get_timestamps() for each_signal_data in signal_data.values()]
        return pd.contact(dfs, axis=1, sort=True)

    def create_new_order_from_signal(self,
                                     symbol: Str,
                                     position: Float,
                                     timestamp: datetime,
                                     allocation: Float,
                                     price: Float,
                                     order_type: OrderType) -> Order:

        amount = abs(int(allocation * position / price))
        if position > 0:
            side = Side.BUY
        else:
            side = Side.SELL
        if order_type == OrderType.MMARKET:
            return MarketOrder(symbol, amount, side, timestamp)
        else:
            return LimitOrder(symbol, amount, side, timestamp)

    @staticmethod
    def get_positions(timestamp: datetime,
                      signal_data: Dict[Str, SignalData],
                      signal_name: Str):
        # positions: pd.Series
        positions = signal_data[signal_name].position(timestamp)
        if not isclose(positions.abs().sum(), 0):
            positions = positions / positions.abs().sum()
        return positions

    def update_order_from_signal(self,
                                 order: Order,
                                 position: Float,
                                 allocation: Float,
                                 price: Float, ) -> Order:
        if isinstance(order, MarketOrder):
            new_order = self.create_new_order_from_signal(order.symbol,
                                                          position,
                                                          order.timestamp,
                                                          allocation,
                                                          price,
                                                          OrderType.MMARKET)
        else:
            new_order = self.create_new_order_from_signal(order.symbol,
                                                          position,
                                                          order.timestamp,
                                                          allocation,
                                                          price,
                                                          OrderType.LIMIT)

        return self.update_order_by_order(order, new_order)

    @staticmethod
    def update_order_by_order(order: Order,
                              new_order: Order) -> Order:

        if order.side == new_order.side:
            order.amount += new_order.amount
        else:
            order.amount -= new_order.amount
            if order.amount < 0:
                order.amount = abs(order.amount)
                if order.side == Side.BUY:
                    order.side = Side.SELL
                else:
                    order.side = Side.BUY
        return order

    def get_order_from_signal(self,
                              orders: Dict[Str, Order],
                              symbol: Str,
                              position: Float,
                              timestamp: datetime,
                              allocation: Float,
                              price: Float,
                              order_type: OrderType) -> Order:

        if symbol not in orders.keys():
            return self.create_new_order_from_signal(symbol,
                                                     position,
                                                     timestamp,
                                                     allocation,
                                                     price,
                                                     order_type)
        else:
            return self.update_order_from_signal(orders[symbol],
                                                 position,
                                                 allocation,
                                                 price)

    def get_orders_from_signal(self,
                               timestamp: datetime,
                               signal_data: Dict[Str, SignalData],
                               row: pd.Series,
                               order_type: OrderType) -> Dict[Str, SignalData]:

        # signal_allocations: Dict[Str, Float]
        signal_allocations = self.risk_policy.get_allocation(self._portfolio, signal_data.keys())
        # orders: Dict[Str, Order]
        orders = dict()
        for signal_name in row.index:
            if row[signal_name]:
                positions = self.get_positions(timestamp, signal_data, signal_name)
                allocation = signal_allocations[signal_name]
                for symbol, position in positions.iteritems():
                    price = prices[symbol]
                    orders[symbol] = self.get_order_from_signal(
                        orders,
                        symbol,
                        position,
                        timestamp,
                        allocation,
                        price,
                        order_type)

        return orders

    # TODO: speed up
    def executing_signals(self, signal_data: Dict[Str, SignalData],
                          order_type: Optional[OrderType] = OrderType.MMARKET):
        if self.portfolio is None:
            logging.error(f"Please use set_portfolio function to set a portfolio at first. ")
        else:
            df_timestamp = self.merge_signal_timestamps(signal_data)
            for timestamp, row in df_timestamp.iterrows():
                orders = self.get_orders_from_signal(timestamp,
                                                     signal_data,
                                                     row,
                                                     order_type)
                self.executing_orders(orders)

    def executing_orders(self,
                         orders: Dict[Str, SignalData],
                         src: Optional[Str] = None,
                         target: Optional[Str] = None):
        if self._portfolio is None:
            logging.error(f"Please use set_portfolio function to set a portfolio at first. ")
        else:
            if len(self._remaining_orders):
                for symbol, order in self._remaining_orders.items():
                    if symbol not in orders.keys():
                        orders[symbol] = copy.deepcopy(self._remaining_orders[symbol])
                    else:
                        orders[symbol] = self.update_order_by_order(orders[symbol], self._remaining_orders[symbol])
            self._remaining_orders = dict()

            for symbol, order in orders.items():
                fills, remaining_order = self.order_manager.accept_order(order, src, target)
                self.record_trade(fills)
                if remaining_order:
                    self.remaining_orders[symbol] = remaining_order

    # def get_trades(self,
    #                source,
    #                symbol,
    #                since,
    #                account_type=AccountType.EXCHANGE):
    #     index = self.trades_index[(source, account_type, symbol)]
    #     i = 0
    #     if since is not None:
    #         i = bisect_left(index, since)
    #     return self.trades[(source, account_type, symbol)][i:]

    def get_portfolio(self) -> Portfolio:
        return copy.deepcopy(self._portfolio)

    def set_portfolio(self, portfolio: Portfolio):
        self._portfolio = copy.deepcopy(portfolio)

    @classmethod
    def get_market_value(cls, source, portfolio, timestamp=None):
        if timestamp is None:
            timestamp = get_current_frame_timestamp()

        if isinstance(portfolio, Portfolio):
            return cls._market_value_spot_portfolio(source, portfolio,
                                                    timestamp)
        elif isinstance(portfolio, MarginPortfolio):
            return cls._market_value_margin_portfolio(source, portfolio,
                                                      timestamp)
        elif isinstance(portfolio, FuturePortfolio):
            return cls._market_value_future_portfolio(source, portfolio,
                                                      timestamp)
        else:
            raise ValueError(f'Unrecognized portfolio type {type(portfolio)}')

    @classmethod
    def _market_value_spot_portfolio(cls, source, portfolio, timestamp):
        market_value = 0.0
        for asset, amount in portfolio.balance.items():
            if asset == portfolio.base_currency:
                market_value += amount
            else:
                # symbol = currency_pair(currency, portfolio.base_currency)
                price = get_context().marketdata.get_prices(
                    source, asset, timestamp)
                if not isinstance(price, list):
                    market_value += amount * price

        return market_value

    # @classmethod
    # def _market_value_margin_portfolio(cls, source, portfolio, timestamp):
    #     ...
    #
    # @classmethod
    # def _market_value_future_portfolio(cls, source, portfolio, timestamp):
    #     ...

# class PortfolioManagerBacktest:
#     def __init__(self, portfolio_by_source=None):
#         if portfolio_by_source is None:
#             portfolio_by_source = {}
#
#         # (source, account_type) -> portfolio
#         self.portfolio_by_source: Dict[Tuple[str, AccountType], Portfolio] = \
#             portfolio_by_source.copy()
#
#         self.trades = defaultdict(list)
#         self.trades_index = defaultdict(list)
#
#     def get_trades(self,
#                    source,
#                    symbol,
#                    since,
#                    account_type=AccountType.EXCHANGE):
#         index = self.trades_index[(source, account_type, symbol)]
#         i = 0
#         if since is not None:
#             i = bisect_left(index, since)
#         return self.trades[(source, account_type, symbol)][i:]
#
#     def record_trade(self, source, trade, account_type=AccountType.EXCHANGE):
#         self.trades[(source, account_type, trade['symbol'])].append(
#             trade)
#         self.trades_index[(source, account_type, trade['symbol'])].append(
#             trade['timestamp'])
#
#     def get_portfolio(self, source, account_type=AccountType.EXCHANGE) -> Portfolio:
#         return self.portfolio_by_source[(source, account_type)]
#
#     def set_portfolio(self,
#                       portfolio,
#                       account_type=AccountType.EXCHANGE):
#         self.portfolio_by_source[(portfolio.source, account_type)] = portfolio
#
#     @classmethod
#     def get_market_value(cls, source, portfolio, timestamp=None):
#         if timestamp is None:
#             timestamp = get_current_frame_timestamp()
#
#         if isinstance(portfolio, Portfolio):
#             return cls._market_value_spot_portfolio(source, portfolio,
#                                                     timestamp)
#         elif isinstance(portfolio, MarginPortfolio):
#             return cls._market_value_margin_portfolio(source, portfolio,
#                                                       timestamp)
#         elif isinstance(portfolio, FuturePortfolio):
#             return cls._market_value_future_portfolio(source, portfolio,
#                                                       timestamp)
#         else:
#             raise ValueError(f'Unrecognized portfolio type {type(portfolio)}')
#
#     @classmethod
#     def _market_value_spot_portfolio(cls, source, portfolio, timestamp):
#         market_value = 0.0
#         for asset, amount in portfolio.balance.items():
#             if asset == portfolio.base_currency:
#                 market_value += amount
#             else:
#                 # symbol = currency_pair(currency, portfolio.base_currency)
#                 price = get_context().marketdata.get_prices(
#                     source, asset, timestamp)
#                 if not isinstance(price, list):
#                     market_value += amount * price
#
#         return market_value
#
#     @classmethod
#     def _market_value_margin_portfolio(cls, source, portfolio, timestamp):
#         ...
#
#     @classmethod
#     def _market_value_future_portfolio(cls, source, portfolio, timestamp):
#         ...
