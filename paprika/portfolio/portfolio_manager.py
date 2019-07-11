import copy
from bisect import bisect_left
from collections import defaultdict
from numbers import Real
from typing import Dict, List, Mapping, NamedTuple, Tuple

from absl import logging

from paprika.core.api import get_current_frame_timestamp
from paprika.core.context import get_context
from paprika.portfolio.account_type import AccountType
from paprika.portfolio.portfolio import (FuturePortfolio, MarginPortfolio,
                                         Portfolio)
from paprika.utils.distribution import Dist
from paprika.utils.types import float_type
from paprika.utils.utils import currency_pair, isclose


class PortfolioManagerBacktest:
    def __init__(self, portfolio_by_source=None):
        if portfolio_by_source is None:
            portfolio_by_source = {}

        # (source, account_type) -> portfolio
        self.portfolio_by_source: Dict[Tuple[str, AccountType], Portfolio] = \
            portfolio_by_source.copy()

        self.trades = defaultdict(list)
        self.trades_index = defaultdict(list)

    def get_trades(self,
                   source,
                   symbol,
                   since,
                   account_type=AccountType.EXCHANGE):
        index = self.trades_index[(source, account_type, symbol)]
        i = 0
        if since is not None:
            i = bisect_left(index, since)
        return self.trades[(source, account_type, symbol)][i:]

    def record_trade(self, source, trade, account_type=AccountType.EXCHANGE):
        self.trades[(source, account_type, trade['symbol'])].append(
            trade)
        self.trades_index[(source, account_type, trade['symbol'])].append(
            trade['timestamp'])

    def get_portfolio(self, source, account_type=AccountType.EXCHANGE) -> Portfolio:
        return self.portfolio_by_source[(source, account_type)]

    def set_portfolio(self,
                      portfolio,
                      account_type=AccountType.EXCHANGE):
        self.portfolio_by_source[(portfolio.source, account_type)] = portfolio

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

    @classmethod
    def _market_value_margin_portfolio(cls, source, portfolio, timestamp):
        ...

    @classmethod
    def _market_value_future_portfolio(cls, source, portfolio, timestamp):
        ...


