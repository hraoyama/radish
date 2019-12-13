from numbers import Real
from typing import Dict, List, NamedTuple, Tuple, Union, Optional
from datetime import datetime
from aenum import Enum
from absl import logging
import pandas as pd

from paprika.utils.distribution import Dist
from paprika.utils.types import float_type
from paprika.utils.utils import isclose
from paprika.data.data_channel import DataChannel
from paprika.execution.order import Side, Order, MarketOrder, ExecutionResult
from paprika.portfolio.account_type import AccountType


class Portfolio:
    def __init__(self,
                 name: str,
                 base_currency: str,
                 balance: Dict[str, Real],
                 avail_balance: Optional[Dict[str, Real]] = None):
        if avail_balance is None:
            self._avail_balance = Dist(self._convert_and_clean_up(balance))
        else:
            self._avail_balance = Dist(self._convert_and_clean_up(avail_balance))
        self._name = name
        self._base_currency = base_currency
        self._balance = Dist(self._convert_and_clean_up(balance))
        self._trades = []
        self._sub_portfolio = {}
        self._portfolio_record = pd.DataFrame(columns=['balance', 'portfolio_value'])

    def __repr__(self):
        return (f'Portfolio(name={self.name}, '
                f'base_currency={self.base_currency}, '
                f'balance={self.balance}),'
                f'avail_balance={self.avail_balance})')

    def __add__(self, other: 'Portfolio') -> 'Portfolio':
        assert (self.base_currency == other.base_currency)
        new_balance = self.balance + other.balance
        new_avail_balance = self.avail_balance + other.avail_balance
        return Portfolio(self.name, self.base_currency, new_balance, new_avail_balance)

    def __sub__(self, other: 'Portfolio') -> 'Portfolio':
        assert (self.base_currency == other.base_currency)
        new_balance = self.balance - other.balance
        new_avail_balance = self.avail_balance - other.avail_balance
        return Portfolio(self.name, self.base_currency, new_balance, new_avail_balance)

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
        self._balance = Dist(self._convert_and_clean_up(self._balance))
        return self._balance.readonly()

    @property
    def avail_balance(self) -> Dist:
        self._avail_balance = Dist(self._convert_and_clean_up(self._avail_balance))
        return self._avail_balance.readonly()

    @property
    def total_balance(self) -> Dist:
        total = self._balance.readonly()
        for _, sub_portfolio in self._sub_portfolio.items():
            total += sub_portfolio.total_balance
        return total

    @property
    def total_avail_balance(self) -> Dist:
        total = self._avail_balance.readonly()
        for _, sub_portfolio in self._sub_portfolio.items():
            total += sub_portfolio.total_avail_balance
        return total

    def copy_balances(self) -> Tuple[Dist, Dist]:
        return self.balance.readonly(), self.avail_balance.readonly()

    @property
    def trades(self) -> List:
        return self._trades.copy()

    @property
    def total_trades(self):
        total = self._trades.copy()
        for _, sub_portfolio in self._sub_portfolio.items():
            total += sub_portfolio.total_trades
        return total

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

    @property
    def total_portfolio_records(self) -> pd.DataFrame:
        # total = {self.name: self._portfolio_record.copy()}
        total = self.portfolio_records
        for name, sub_portfolio in self._sub_portfolio.items():
            # total = total.append(sub_portfolio.total_portfolio_records)
            sub = sub_portfolio.total_portfolio_records
            t_index = total.index.append(sub.index).unique()
            total = total.reindex(t_index)
            total.loc[total.balance.isna(), 'balance'] = [Dist()]
            total.loc[total.portfolio_value.isna(), 'portfolio_value'] = 0
            total.loc[sub.index] += sub
            # total += sub_portfolio.total_portfolio_records
            # total[name] = sub_portfolio.total_portfolio_records
        return total

    def add_portfolio_records(self, timestamp: datetime):
        new_record = pd.DataFrame({'balance': [self.balance],
                                   'portfolio_value': [self.portfolio_value_in_base_currency(timestamp)]},
                                  index=[timestamp])
        self._portfolio_record = self._portfolio_record.append(new_record)

    def get_sub_portfolio(self, sub_portfolio_name: str) -> 'Portfolio':
        return self._sub_portfolio[sub_portfolio_name]

    def add_sub_portfolio(self, sub_portfolio: 'Portfolio'):
        self._sub_portfolio[sub_portfolio.name] = sub_portfolio

    def list_sub_portfolio(self) -> List:
        return list(self._sub_portfolio.keys())

    def total_portfolio_value_in_base_currency(self, timestamp: datetime) -> float:
        total_value = self.portfolio_value_in_base_currency(timestamp)
        for _, sub_portfolio in self._sub_portfolio.items():
            total_value += sub_portfolio.get_portfolio_value_in_base_currency(timestamp)
        return total_value

    def portfolio_value_in_base_currency(self, timestamp: datetime) -> float:
        return self.get_value_distribution_in_base_currency(timestamp).sum()

    def get_value_distribution_in_base_currency(self, timestamp: datetime) -> Dist:
        value_dist = Dist()
        for asset, amount in self._balance.items():
            if asset == self._base_currency:
                value_dist[asset] = float_type()(amount)
            else:
                price = DataChannel.fetch_price([asset], timestamp)
                if price is not None:
                    value_dist[asset] = float_type()(price.Price.values * amount)
        return value_dist

    @staticmethod
    def _convert_and_clean_up(balance):
        return {
            symbol: float_type()(amount)
            for symbol, amount in balance.items() if not isclose(float_type()(amount), 0)
        }


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


class MarginPosition:
    pass


class MarginPortfolio:
    pass



