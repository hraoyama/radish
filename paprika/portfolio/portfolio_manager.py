import copy
from bisect import bisect_left
from collections import defaultdict, OrderedDict
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
from paprika.signals.signal_data import SignalData
from paprika.data.fetcher import HistoricalDataFetcher


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
        self.fetcher = HistoricalDataFetcher()

    @property
    def portfolio(self) -> Portfolio:
        return self._portfolio

    def set_portfolio(self,
                      portfolio: Portfolio,
                      timestamp: datetime):
        self._portfolio = portfolio
        self._portfolio.add_portfolio_records(timestamp)

    @property
    def trades(self):
        return self._portfolio.total_trades

    @property
    def portfolio_records(self):
        return self._portfolio.portfolio_records

    @staticmethod
    def merge_signal_timestamps(signal_data: Dict[str, SignalData]):
        dfs = [each_signal_data.get_timestamps() for each_signal_data in signal_data.values()]
        return pd.contact(dfs, axis=1, sort=True)

    # @staticmethod
    # def create_new_order_from_signal(symbol: Str,
    #                                  position: Float,
    #                                  timestamp: datetime,
    #                                  allocation: Float,
    #                                  price: Float,
    #                                  order_type: OrderType) -> Order:
    #
    #     amount = abs(int(allocation * position / price))
    #     if position > 0:
    #         side = Side.BUY
    #     else:
    #         side = Side.SELL
    #     if order_type == OrderType.MMARKET:
    #         return MarketOrder(symbol, amount, side, timestamp)
    #     else:
    #         return LimitOrder(symbol, amount, side, timestamp)

    @staticmethod
    def get_positions(timestamp: datetime,
                      signal_data: Dict[str, SignalData],
                      signal_name: str):
        # positions: pd.Series
        positions = signal_data[signal_name].position(timestamp)
        if not isclose(positions.abs().sum(), 0):
            positions = positions / positions.abs().sum()
        return positions

    # def update_order_from_signal(self,
    #                              order: Order,
    #                              position: Float,
    #                              allocation: Float,
    #                              price: Float, ) -> Order:
    #     if isinstance(order, MarketOrder):
    #         new_order = self.create_new_order_from_signal(order.symbol,
    #                                                       position,
    #                                                       order.timestamp,
    #                                                       allocation,
    #                                                       price,
    #                                                       OrderType.MMARKET)
    #     else:
    #         new_order = self.create_new_order_from_signal(order.symbol,
    #                                                       position,
    #                                                       order.timestamp,
    #                                                       allocation,
    #                                                       price,
    #                                                       OrderType.LIMIT)
    #
    #     return self.update_order_by_order(order, new_order)

    # @staticmethod
    # def update_order_by_order(order: Order,
    #                           new_order: Order) -> Order:
    #
    #     if order.side == new_order.side:
    #         order.amount += new_order.amount
    #     else:
    #         order.amount -= new_order.amount
    #         if order.amount < 0:
    #             order.amount = abs(order.amount)
    #             if order.side == Side.BUY:
    #                 order.side = Side.SELL
    #             else:
    #                 order.side = Side.BUY
    #     return order

    # def get_order_from_signal(self,
    #                           orders: Dict[Str, Order],
    #                           symbol: Str,
    #                           position: Float,
    #                           timestamp: datetime,
    #                           allocation: Float,
    #                           price: Float,
    #                           order_type: OrderType) -> Order:
    #
    #     if symbol not in orders.keys():
    #         return self.create_new_order_from_signal(symbol,
    #                                                  position,
    #                                                  timestamp,
    #                                                  allocation,
    #                                                  price,
    #                                                  order_type)
    #     else:
    #         return self.update_order_from_signal(orders[symbol],
    #                                              position,
    #                                              allocation,
    #                                              price)

    # def get_orders_from_signal(self,
    #                            timestamp: datetime,
    #                            signal_data: Dict[Str, SignalData],
    #                            row: pd.Series,
    #                            order_type: OrderType) -> Dict[Str, SignalData]:
    #
    #     # signal_allocations: Dict[Str, Float]
    #     signal_allocations = self.risk_policy.get_allocation(self._portfolio, signal_data.keys())
    #     # orders: Dict[Str, Order]
    #     orders = dict()
    #     for signal_name in row.index:
    #         if row[signal_name]:
    #             positions = self.get_positions(timestamp, signal_data, signal_name)
    #             allocation = signal_allocations[signal_name]
    #             for symbol, position in positions.iteritems():
    #                 price = prices[symbol]
    #                 orders[symbol] = self.get_order_from_signal(
    #                     orders,
    #                     symbol,
    #                     position,
    #                     timestamp,
    #                     allocation,
    #                     price,
    #                     order_type)
    #
    #     return orders

    # TODO: speed up
    def executing_signals(self, signal_data: Dict[str, SignalData],
                          order_type: Optional[OrderType] = OrderType.MARKET):
        if self._portfolio is None:
            logging.error(f"Please use set_portfolio function to set a portfolio at first. ")
        else:
            signals_timestamp = self.merge_signal_timestamps(signal_data)
            for timestamp, signal_names in signals_timestamp.iterrows():
                # orders = self.get_orders_from_signal(timestamp,
                #                                      signal_data,
                #                                      signal_names,
                #                                      order_type)
                # self.executing_orders(orders)
                self.executing_signals_at_one_timestamp(timestamp,
                                                        signal_data,
                                                        signal_names,
                                                        order_type)

    def executing_signals_at_one_timestamp(self,
                                           timestamp: datetime,
                                           signal_data: Dict[str, SignalData],
                                           signal_names: pd.Series,
                                           order_type: OrderType):

        orders = self.get_orders_from_signals(timestamp,
                                              signal_data,
                                              signal_names,
                                              order_type)
        self.executing_orders_at_one_timestamp(orders)

    def get_orders_from_signals(self,
                                timestamp: datetime,
                                signal_data: Dict[str, SignalData],
                                signal_names: pd.Series,
                                order_type: OrderType) -> Dict[str, SignalData]:
        orders = dict()
        for signal_name in signal_names.index:
            if signal_names[signal_name]:
                orders = self.get_orders_from_signal(timestamp,
                                                     signal_data,
                                                     signal_name,
                                                     order_type)
        return orders

    def get_orders_from_signal(self,
                               timestamp: datetime,
                               signal_data: Dict[str, SignalData],
                               signal_name: pd.Series,
                               order_type: OrderType) -> Dict[str, SignalData]:

        signal_portfolio = self._portfolio[signal_name]
        signal_portfolio_dist = signal_portfolio.dist()

        target_dist = self.get_positions(timestamp, signal_data, signal_name)
        diff_dist = target_dist - signal_portfolio_dist
        diff_in_base = diff_dist * signal_portfolio.portfolio_value()
        prices = signal_data[signal_name].prices(timestamp)
        diff_in_amount = diff_in_base / prices

        for symbol, amount in diff_in_amount.iteritems():
            orders[symbol] = self.get_order_from_signal(
                orders,
                symbol,
                amount,
                timestamp,
                order_type)

        return orders

    def get_order_from_signal(self,
                              orders: Dict[str, Order],
                              symbol: str,
                              amount: int,
                              timestamp: datetime,
                              order_type: OrderType) -> Order:

        if symbol not in orders.keys():
            return self.create_new_order_from_signal(symbol,
                                                     amount,
                                                     timestamp,
                                                     order_type)
        else:
            return self.update_order_from_signal(orders[symbol],
                                                 amount)

    @staticmethod
    def create_new_order_from_signal(symbol: str,
                                     amount: int,
                                     timestamp: datetime,
                                     order_type: OrderType) -> Order:

        if amount > 0:
            side = Side.BUY
        else:
            side = Side.SELL
        if order_type == OrderType.MMARKET:
            return MarketOrder(symbol, amount, side, timestamp)
        else:
            return LimitOrder(symbol, amount, side, timestamp)

    def update_order_from_signal(self,
                                 order: Order,
                                 amount: int) -> Order:
        if isinstance(order, MarketOrder):
            new_order = self.create_new_order_from_signal(order.symbol,
                                                          amount,
                                                          order.timestamp,
                                                          OrderType.MMARKET)
        else:
            new_order = self.create_new_order_from_signal(order.symbol,
                                                          amount,
                                                          order.timestamp,
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

    def merge_orders(self,
                     orders: Dict[str, Order],
                     other_orders: Dict[str, Order]) -> Dict[str, Order]:
        for symbol, order in other_orders.items():
            if symbol not in orders.keys():
                orders[symbol] = copy.deepcopy(other_orders[symbol])
            else:
                orders[symbol] = self.update_order_by_order(orders[symbol], other_orders[symbol])
        return orders

    def executing_orders_at_one_timestamp(self,
                                          orders: Dict[str, Order],
                                          portfolio_to_use: Optional[Portfolio] = None,
                                          src: Optional[str] = None,
                                          target: Optional[str] = None):
        if self._portfolio_exist():
            if portfolio_to_use is None:
                portfolio_to_use = self._portfolio

            if len(self._remaining_orders):
                orders = self.merge_orders(orders, self._remaining_orders)
                self._remaining_orders = {}

            orders = self.sort_orders(orders)

            for symbol, order in orders.items():
                if self.is_enough_balance(order, portfolio_to_use):
                    logging.info(f'Send {order} to Order Manager for portfolio {portfolio_to_use.name}.')
                    portfolio_to_use, remaining_order = self.order_manager.accept_order(portfolio_to_use, order, src, target)
                    if remaining_order:
                        self._remaining_orders[symbol] = remaining_order
                else:
                    logging.info(f'Portfolio {portfolio_to_use.name} have {portfolio_to_use.balance}.'
                                 f'Can not support order to {order.side} {order.symbol} {order.amount}.')

    def _portfolio_exist(self) -> bool:
        if self._portfolio is None:
            raise Exception(f"Please use set_portfolio function to set a portfolio at first. ")
        else:
            return True

    # def record_trade(self,
    #                  fills: List[ExecutionResult],
    #                  portfolio_to_use: Optional[Portfolio] = None) -> bool:
    #     if self._portfolio_exist():
    #         if portfolio_to_use is None:
    #             portfolio_to_use = self._portfolio
    #         self._executed_orders.append(fills)
    #         self._portfolio.add_trades(fills)
    #         portfolio_to_use.add_trades(fills)
    #         for fill in fills:
    #             portfolio_to_use.balance[portfolio_to_use.base_currency] -= fill.costs
    #             if fill.side == Side.BUY:
    #                 portfolio_to_use.balance[fill.symbol] += fill.amount
    #             else:
    #                 portfolio_to_use.balance[fill.symbol] -= fill.amount
    #         return True

    @staticmethod
    def sort_orders(orders: Dict[str, Order]) -> OrderedDict:
        """
        Sell first, then buy
        """
        new_orders = OrderedDict()
        for symbol, order in orders.items():
            if order.side == Side.SELL:
                new_orders[symbol] = order
        for symbol, order in orders.items():
            if order.side == Side.BUY:
                new_orders[symbol] = order

        return new_orders

    def is_enough_balance(self,
                          order: Order,
                          portfolio_to_use: Optional[Portfolio] = None) -> bool:
        if self._portfolio_exist():
            if portfolio_to_use is None:
                portfolio_to_use = self._portfolio
            if order.side == Side.SELL:
                return True if portfolio_to_use.balance[order.symbol] >= order.amount else False
            if order.side == Side.BUY:
                price = self.fetcher.fetch_price_at_timestamp(order.symbol, order.creation_time)
                base_balance = portfolio_to_use.balance[portfolio_to_use.base_currency]
                able_to_buy = int(base_balance / price)
                return True if able_to_buy >= order.amount else False

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

    # @classmethod
    # def get_market_value(cls, source, portfolio, timestamp=None):
    #     if timestamp is None:
    #         timestamp = get_current_frame_timestamp()
    #
    #     if isinstance(portfolio, Portfolio):
    #         return cls._market_value_spot_portfolio(source, portfolio,
    #                                                 timestamp)
    #     elif isinstance(portfolio, MarginPortfolio):
    #         return cls._market_value_margin_portfolio(source, portfolio,
    #                                                   timestamp)
    #     elif isinstance(portfolio, FuturePortfolio):
    #         return cls._market_value_future_portfolio(source, portfolio,
    #                                                   timestamp)
    #     else:
    #         raise ValueError(f'Unrecognized portfolio type {type(portfolio)}')
    #
    # @classmethod
    # def _market_value_spot_portfolio(cls, source, portfolio, timestamp):
    #     market_value = 0.0
    #     for asset, amount in portfolio.balance.items():
    #         if asset == portfolio.base_currency:
    #             market_value += amount
    #         else:
    #             symbol = currency_pair(currency, portfolio.base_currency)
    #             price = get_context().marketdata.get_prices(
    #                 source, asset, timestamp)
    #             if not isinstance(price, list):
    #                 market_value += amount * price
    #
    #     return market_value

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
