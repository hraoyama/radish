from typing import Callable, Dict, List

from paprika.core.context import get_context
from paprika.exchange.constants import DEFAULT_OHLCV_LIMIT
from paprika.execution.order import OrderType
from paprika.portfolio.account_type import AccountType


def register_timer(handler: Callable, frequency: str):
    return get_context().event_engine.register_timer(handler, frequency)


def get_ohlcv(source: str,
              symbol: str,
              frequency: str,
              fields: List[str] = None,
              end: int = None,
              limit: int = DEFAULT_OHLCV_LIMIT,
              partial: bool = False):
    return get_context().marketdata.get_ohlcv(
        source,
        symbol,
        frequency,
        fields=fields,
        end_millis=end,
        limit=limit,
        partial=partial)


def get_current_frame_timestamp():
    return get_context().clock.current_frame_timestamp()


def get_markets(source: str):
    return get_context().marketdata.get_markets(source)


def get_orderbook(source: str,
                  symbol: str,
                  depth: int = 5,
                  end: int = None):
    return get_context().marketdata.get_orderbook(source,
                                                  symbol,
                                                  depth,
                                                  end_millis=end)


def get_tickers(source: str):
    return get_context().marketdata.get_tickers(source)


def get_max_price(source,
                  symbol,
                  frequency,
                  timestamp: int = None,
                  window=120):
    return get_context().marketdata.get_max_price(
        source, symbol, frequency, timestamp, window)


def get_ticker_price(source: str, symbol: str):
    return get_context().marketdata.get_ticker_price(source, symbol)


def get_portfolio(source: str, account_type: str = AccountType.EXCHANGE):
    return get_context().portfolio_manager.get_portfolio(source, account_type)


def place_order(source: str, order, account_type: str = AccountType.EXCHANGE):
    return get_context().execution_service.place_order(source, order,
                                                       account_type)


def get_open_orders(source: str, symbol: str, account_type: str = AccountType.EXCHANGE):
    return get_context().execution_service.get_open_orders(source, symbol, account_type)


def cancel_order(source: str, order, account_type: str = AccountType.EXCHANGE):
    return get_context().execution_service.cancel_order(source, order, account_type)


def get_my_trades(source: str, symbol, since: int = None, account_type: str = AccountType.EXCHANGE):
    return get_context().execution_service.get_my_trades(source, symbol, since, account_type)


def order_target_percent(source,
                         asset,
                         percent,
                         order_type=OrderType.MARKET,
                         account_type: str = AccountType.EXCHANGE):
    return get_context().execution_service.order_target_percent(
        source, asset, percent, order_type, account_type)


def order_target_percents(source,
                          percents: Dict[str, float],
                          order_type=OrderType.MARKET,
                          account_type: str = AccountType.EXCHANGE):
    return get_context().execution_service.order_target_percents(
        source, percents, order_type, account_type)
