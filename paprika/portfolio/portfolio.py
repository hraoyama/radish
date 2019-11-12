from numbers import Real
from typing import Dict, List, NamedTuple, Tuple, Union
from datetime import datetime

from absl import logging
import pandas as pd

from paprika.core.context import get_context
from paprika.utils.distribution import Dist
from paprika.utils.types import float_type
from paprika.utils.utils import isclose
from paprika.data.fetcher import HistoricalDataFetcher
from paprika.execution.order import Side, Order, MarketOrder, ExecutionResult


class Portfolio:
    def __init__(self,
                 name: str,
                 base_currency: str,
                 balance: Dict[str, Real]):
        self._name = name
        self._base_currency = base_currency
        self._balance = Dist(self._convert_and_clean_up(balance))
        self._trades = []
        self._sub_portfolio = {}
        self.fetcher = HistoricalDataFetcher()
        self._portfolio_record = pd.DataFrame(columns=['balance', 'portfolio_value'])

    def __repr__(self):
        return (f'Portfolio(name={self.name}, '
                f'base_currency={self.base_currency}, '
                f'balance={self.balance})')

    def __add__(self, value: 'Portfolio') -> 'Portfolio':
        assert (self.base_currency == value.base_currency)
        new_balance = self.balance + value.balance
        return Portfolio(self.base_currency, new_balance)

    def __sub__(self, value: 'Portfolio') -> 'Portfolio':
        assert (self.base_currency == value.base_currency)
        new_balance = self.balance - value.balance
        return Portfolio(self.base_currency, new_balance)

    @property
    def name(self):
        return self._name

    @property
    def base_currency(self) -> str:
        return self._base_currency

    def clear_sub_portfolio(self):
        self._sub_portfolio = {}

    @property
    def balance(self) -> Dist:
        return self._balance.readonly()

    @property
    def total_balance(self) -> Dist:
        total_balance = self._balance.readonly()
        for _, sub_portfolio in self._sub_portfolio.items():
            total_balance += sub_portfolio.balance
        return total_balance

    def copy_balances(self) -> Dist:
        return self.balance.readonly()

    @property
    def trades(self) -> List:
        return self._trades.copy()

    @property
    def total_trades(self):
        total_trades = self._trades.copy()
        for _, sub_portfolio in self._sub_portfolio.items():
            total_trades += sub_portfolio.trades
        return total_trades

    def record_trades(self, fills: List[ExecutionResult]):
        for fill in fills:
            if isinstance(fill, ExecutionResult):
                self._balance[self.base_currency] -= fill.costs
                if fill.side == Side.BUY:
                    self._balance[fill.symbol] += fill.amount
                else:
                    self._balance[fill.symbol] -= fill.amount
                self._trades.append(fill)

    @property
    def portfolio_records(self) -> pd.DataFrame:
        return self._portfolio_record.copy()

    def add_portfolio_records(self, timestamp: datetime):
        new_record = pd.DataFrame({'balance': [self.balance],
                                   'portfolio_value': [self.portfolio_value(timestamp)]},
                                  index=[timestamp])
        self._portfolio_record = self._portfolio_record.append(new_record)

    def get_sub_portfolio(self, sub_portfolio_name: str) -> 'Portfolio':
        return self._sub_portfolio[sub_portfolio_name]

    def add_sub_portfolio(self, sub_portfolio: 'Portfolio'):
        self._sub_portfolio[sub_portfolio.name] = sub_portfolio

    def list_sub_portfolio(self) -> List:
        return list(self._sub_portfolio.keys())

    def portfolio_value(self, timestamp: datetime) -> float:
        total_value = self.get_portfolio_value_in_base_currency(timestamp)
        for _, sub_portfolio in self._sub_portfolio.items():
            total_value += sub_portfolio.get_portfolio_value_in_base_currency(timestamp)
        return total_value

    def get_portfolio_value_in_base_currency(self, timestamp: datetime) -> float:
        value_dist = self.get_value_distribution_in_base_currency(timestamp)

        return value_dist.sum()

    def get_value_distribution_in_base_currency(self, timestamp: datetime) -> Dist:
        value_dist = Dist()
        for asset, amount in self._balance.items():
            if asset == self._base_currency:
                value_dist[asset] = float_type()(amount)
            else:
                price = self.fetcher.fetch_price_at_timestamp(asset, timestamp)

                value_dist[asset] = float_type()(price * amount)
                # if not isinstance(limit_price, list):
                #     value_dist[asset] = float_type()(limit_price * amount)
                # else:
                #     value_dist[asset] = None
        return value_dist

    @staticmethod
    def _convert_and_clean_up(balance):
        return {
            symbol: float_type()(amount)
            for symbol, amount in balance.items() if not isclose(float_type()(amount), 0)
        }


# class Portfolio:
#     def __init__(self,
#                  source: str,
#                  base_currency: str,
#                  balance: Dict[str, Real],
#                  avail_balance: Dict[str, Real] = None):
#         if avail_balance is None:
#             avail_balance = balance.copy()
# 
#         self._source = source
#         self._base_currency = base_currency
#         self._balance = Dist(self._convert_and_clean_up(balance))
#         self._avail_balance = Dist(self._convert_and_clean_up(avail_balance))
#         self._trades = []
# 
#     @property
#     def source(self):
#         return self._source
# 
#     @property
#     def base_currency(self):
#         return self._base_currency
# 
#     @property
#     def balance(self):
#         return self._balance.readonly()
# 
#     @property
#     def avail_balance(self):
#         return self._avail_balance.readonly()
# 
#     @property
#     def trades(self):
#         return self._trades
# 
#     def __repr__(self):
#         return (f'Portfolio(source={self._source}, '
#                 f'base_currency={self._base_currency}, '
#                 f'balance={self._balance}, '
#                 f'avail_balance={self._avail_balance})')
# 
#     def copy_balances(self) -> (Dist, Dist):
#         return self._balance.readonly(), self._avail_balance.readonly()
# 
#     def get_value_distribution_in_base_currency(self) -> Dist:
#         value_dist = Dist()
#         # for currency, amount in self._balance.items():
#         for asset, amount in self._balance.items():
#             if asset == self._base_currency:
#                 value_dist[asset] = float_type()(amount)
#             else:
#                 price = get_context().marketdata.get_ticker_price(
#                     self._source, asset)
#                 # value_dist[asset] = get_context().marketdata.get_market_value(self._source, asset, amount)
#                 _, orderbook = get_context().marketdata.get_orderbook(self._source, asset, depth=1)
#                 if len(orderbook) > 0:
#                     price = float_type()(sum(orderbook[0:2]) / 2.0)
#                 else:
#                     price = get_context().marketdata.get_ticker_price(
#                         self._source, asset)
#                 value_dist[asset] = float_type()(price * amount)
#                 # if not isinstance(limit_price, list):
#                 #     value_dist[asset] = float_type()(limit_price * amount)
#                 # else:
#                 #     value_dist[asset] = None
#         return value_dist
# 
#     def __add__(self, value: 'Portfolio') -> 'Portfolio':
#         assert(self._source == value._source)
#         assert(self._base_currency == value._base_currency)
#         new_balance = self.balance + value.balance
#         new_avail_balance = self.avail_balance + value.avail_balance
#         return Portfolio(self._source, self._base_currency, new_balance, new_avail_balance)
# 
#     def __sub__(self, value: 'Portfolio') -> 'Portfolio':
#         assert(self._source == value._source)
#         assert(self._base_currency == value._base_currency)
#         new_balance = self.balance - value.balance
#         new_avail_balance = self.avail_balance - value.avail_balance
#         return Portfolio(self._source, self._base_currency, new_balance, new_avail_balance)
# 
#     @staticmethod
#     def _convert_and_clean_up(balance):
#         return {
#             symbol: float_type()(amount)
#             for symbol, amount in balance.items() if not isclose(float_type()(amount), 0)
#         }


class MarginPosition:
    pass


class MarginPortfolio:
    pass


class FuturePosition(NamedTuple):
    symbol: str
    amount: Real
    margin: Real
    leverage: Real
    entry_price: Real
    liquid_price: Real
    last_price: Real


class FuturePortfolio(NamedTuple):
    margin_balance: Real
    avail_balance: Real
    positions: List[FuturePosition]
