from numbers import Real
from typing import Dict, List, NamedTuple, Tuple
from datetime import datetime

from absl import logging

from paprika.core.context import get_context
from paprika.utils.distribution import Dist
from paprika.utils.types import float_type
from paprika.utils.utils import isclose
from paprika.data.fetcher import HistoricalDataFetcher


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

    @property
    def name(self):
        return self._name

    @property
    def base_currency(self):
        return self._base_currency

    @property
    def balance(self):
        total_balance = self._balance.readonly()
        for _, sub_portfolio in self._sub_portfolio.items():
            total_balance += sub_portfolio.balance
        return total_balance

    @property
    def trades(self):
        total_trades = self._trades
        for _, sub_portfolio in self._sub_portfolio.items():
            total_trades += sub_portfolio.trades
        return total_trades

    def __repr__(self):
        return (f'Portfolio(name={self.name}, '
                f'base_currency={self.base_currency}, '
                f'balance={self.balance})',
                f'sub portfolios={self._sub_portfolio.keys()})')

    def __getitem__(self, name: str) -> 'Portfolio':
        return self._sub_portfolio[name]

    def list_sub_portfolio(self):
        return self._sub_portfolio.keys()

    def copy_balances(self) -> Dist:
        return self.balance.readonly()

    def portfolio_value(self, timestamp: datetime) -> Dict:
        total_value = self.get_portfolio_value_in_base_currency(timestamp)
        for _, sub_portfolio in self._sub_portfolio.items():
            total_value += sub_portfolio.get_portfolio_value_in_base_currency(timestamp)
        return total_value

    def get_portfolio_value_in_base_currency(self, timestamp: datetime) -> Dict:
        value_dist = self.get_value_distribution_in_base_currency(timestamp)

        return value_dist.sum()

    def add_sub_portfolio(self, sub_portfolio: 'Portfolio'):
        self._sub_portfolio[sub_portfolio.name] = sub_portfolio

    def get_value_distribution_in_base_currency(self, timestamp: datetime) -> Dist:
        value_dist = Dist()
        for asset, amount in self._balance.items():
            if asset == self._base_currency:
                value_dist[asset] = float_type()(amount)
            else:
                price = self.fetcher.fetch_price(asset, timestamp)

                value_dist[asset] = float_type()(price * amount)
                # if not isinstance(limit_price, list):
                #     value_dist[asset] = float_type()(limit_price * amount)
                # else:
                #     value_dist[asset] = None
        return value_dist

    def __add__(self, value: 'Portfolio') -> 'Portfolio':
        assert (self.base_currency == value.base_currency)
        new_balance = self.balance + value.balance
        return Portfolio(self.base_currency, new_balance)

    def __sub__(self, value: 'Portfolio') -> 'Portfolio':
        assert (self.base_currency == value.base_currency)
        new_balance = self.balance - value.balance
        return Portfolio(self.base_currency, new_balance)

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
