from typing import Dict
import math
import numpy as np
from absl import logging, flags

from paprika.core.api import get_current_frame_timestamp
from paprika.core.context import get_context
from paprika.execution import order_target
from paprika.execution.order import MarketOrder, OrderType
from paprika.portfolio.account_type import AccountType
from paprika.portfolio.portfolio import Portfolio
from paprika.utils.time import micros_to_datetime
from paprika.exchange.constants import ORDERBOOK_COLUMN_INDICES, DEFAULT_MIN_AMOUNT


class ExecutionServiceBacktest:
    def place_order(self,
                    source,
                    order,
                    account_type: AccountType = AccountType.EXCHANGE):
        if account_type == AccountType.EXCHANGE:
            SpotExecutionSimulator(source, account_type).place_order(order)
        else:
            raise NotImplementedError

    def order_target_percent(self,
                             source,
                             asset,
                             percent,
                             order_type=OrderType.MARKET,
                             account_type: AccountType = AccountType.EXCHANGE):
        if account_type == AccountType.EXCHANGE:
            logging.info(micros_to_datetime(get_current_frame_timestamp()))
            return SpotExecutionSimulator(source, account_type).order_target_percent(
                asset, percent, order_type)
        else:
            raise NotImplementedError

    def order_target_percents(self,
                              source,
                              percents: Dict[str, float],
                              order_type=OrderType.MARKET,
                              account_type: AccountType = AccountType.EXCHANGE):
        if account_type == AccountType.EXCHANGE:
            logging.info(micros_to_datetime(get_current_frame_timestamp()))
            return SpotExecutionSimulator(source, account_type).order_target_percents(
                percents, order_type)
        else:
            raise NotImplementedError

    def get_my_trades(self,
                      source,
                      symbol,
                      since,
                      account_type: AccountType = AccountType.EXCHANGE):
        if account_type == AccountType.EXCHANGE:
            return SpotExecutionSimulator(source, account_type).get_my_trades(symbol, since)
        else:
            raise NotImplementedError

    def cancel_order(self, source, order, account_type):
        return None

    def get_open_orders(self, source, symbol, account_type):
        return []


class SpotExecutionSimulator:
    def __init__(self, source: str, account_type: AccountType):
        self._source = source
        self._account_type = account_type

    def place_order(self, order):

        # logging.info(
        #     f'Place order {order} on {(self._source, self._account_type.name)}')

        if np.isclose(order.amount, 0):
            logging.info(f'Drop order {order} becasue its size '
                         f'{order.amount} is too small.')
            return

        if isinstance(order, MarketOrder):
            self._execute_market_order(order)
            self._record_trades(order)
        else:
            raise NotImplementedError

    def order_target_percent(self,
                             asset,
                             percent,
                             order_type,
                             market_value=None):
        order_target.order_target_percent(
            asset,
            percent,
            order_type,
            self._get_porfolio,
            self._get_market_amount,
            self.place_order)

    def order_target_percents(self, percents, order_type):
        order_target.order_target_percents(
            percents,
            order_type,
            self._get_porfolio,
            self._get_market_amount,
            self.place_order)

    def _old_execute_market_order(self, order):
        portfolio = self._get_porfolio()

        price = get_context().marketdata.get_prices(
            self._source, order.symbol, get_current_frame_timestamp())

        # base, quote = parse_currency_pair(order.symbol)
        quote = portfolio.base_currency
        base = order.symbol
        fee = get_context().config.transaction_fee

        if order.amount >= 0:
            if portfolio.balance[base] >= 0:
                base_delta = order.amount * (1.0 - fee)
                quote_delta = -(order.amount * price)
            else:
                base_delta = order.amount
                quote_delta = -order.amount * price / (1.0 - fee)
        else:
            if portfolio.balance[base] >= 0:
                base_delta = order.amount
                quote_delta = -order.amount * price * (1.0 - fee)
            else:
                base_delta = order.amount / (1.0 - fee)
                quote_delta = -order.amount * price

        new_balance, new_avail_balance = portfolio.copy_balances()
        new_balance[base] += base_delta
        new_avail_balance[base] += base_delta
        new_balance[quote] += quote_delta
        new_avail_balance[quote] += quote_delta

        new_portfolio = Portfolio(self._source,
                                  portfolio.base_currency,
                                  new_balance,
                                  new_avail_balance)

        get_context().portfolio_manager.set_portfolio(new_portfolio)

        logging.debug(f'Executed market order: '
                      f'{base}: {base_delta}, {quote}: {quote_delta}')

    def _execute_market_order(self, order):
        portfolio = self._get_porfolio()

        quote = portfolio.base_currency
        base = order.symbol
        fee = get_context().config.transaction_fee

        amount = order.amount - order.amount % DEFAULT_MIN_AMOUNT

        if amount >= 0:
            quote_delta = self._get_market_value(order.symbol, amount)
            if quote_delta * (1 + fee) > portfolio.avail_balance[quote]:
                logging.info(f'Can not buy {amount} {base} valued as {quote_delta} '
                             f'when only has {portfolio.avail_balance[quote]} {quote}'
                             )
                quote_delta = portfolio.avail_balance[quote] / (1 + fee)
                amount = self._get_market_amount(order.symbol, quote_delta)
                logging.info(f'Change to buy {amount} {base } valued as {quote_delta}')
            base_delta = amount
            quote_delta = -quote_delta * (1.0 + fee)
        else:
            if amount < -portfolio.avail_balance[base]:
                logging.info(f'Can not sell {amount} {base} '
                             f'when only has {-portfolio.avail_balance[base]}')
                amount = max(-portfolio.avail_balance[base], amount)
                logging.info(f'Change to sell {amount} {base }')
            quote_delta = self._get_market_value(order.symbol, amount)
            base_delta = amount
            quote_delta = -quote_delta * (1.0 - fee)
        order.amount = amount
        logging.info(
            f'Place order {order} on {(self._source, self._account_type.name)}')
        new_balance, new_avail_balance = portfolio.copy_balances()
        new_balance[base] += base_delta
        new_avail_balance[base] += base_delta
        new_balance[quote] += quote_delta
        new_avail_balance[quote] += quote_delta

        new_portfolio = Portfolio(self._source,
                                  portfolio.base_currency,
                                  new_balance,
                                  new_avail_balance)

        get_context().portfolio_manager.set_portfolio(new_portfolio)

        logging.debug(f'Executed market order: '
                      f'{base}: {base_delta}, {quote}: {quote_delta}')

    def _record_trades(self, order):
        trade = {'symbol': order.symbol, 'id': order.id}
        fee = get_context().config.transaction_fee
        price = get_context().marketdata.get_prices(
            self._source, order.symbol, get_current_frame_timestamp())
        if order.amount > 0:
            trade['amount'] = order.amount * (1 - fee)
            trade['cost'] = order.amount * price
        else:
            trade['amount'] = - order.amount
            trade['cost'] = - order.amount * price * (1 - fee)
        if order.amount >= 0:
            trade['side'] = 'buy'
        else:
            trade['side'] = 'sell'
        trade['timestamp'] = get_current_frame_timestamp()

        get_context().portfolio_manager.record_trade(
            self._source, trade, self._account_type)

    def get_my_trades(self, symbol, since):
        return get_context().portfolio_manager.get_trades(
            self._source, symbol, since, self._account_type)

    def _get_porfolio(self):
        return get_context().portfolio_manager.get_portfolio(
            self._source, self._account_type)

    def _get_ticker_price(self, symbol):
        return get_context().marketdata.get_ticker_price(
            self._source, symbol)

    def _get_market_value(self, symbol, amount):
        return get_context().marketdata.get_market_value(self._source, symbol, amount)

    def _get_market_amount(self, symbol, value):
        return get_context().marketdata.get_market_amount(self._source, symbol, value)



class MarginExecutionSimulator:
    pass


class FutureExecutionSimulator:
    pass
