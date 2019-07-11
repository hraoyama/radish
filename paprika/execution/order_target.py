from typing import Dict

import numpy as np
from absl import app, logging, flags

from paprika.execution.order import ExecutionResult, MarketOrder, OrderType
from paprika.portfolio.account_type import AccountType
from paprika.portfolio.portfolio import Portfolio
from paprika.utils.distribution import Dist, NormDist
from paprika.utils.types import float_type
from paprika.utils.utils import (currency_pair, isclose, parse_currency_pair,
                                 percentage)


def order_target_percent(asset,
                         percent,
                         order_type,
                         portfolio_getter: callable,
                         price_getter: callable,
                         place_order_executor: callable):
    if order_type != OrderType.MARKET:
        raise ValueError(f'Unsupported order type {order_type} for '
                         f'order_target_percents.')

    percent = float_type()(percent)

    portfolio = portfolio_getter()
    base_currency = portfolio.base_currency

    value_dist = (
        portfolio.get_value_distribution_in_base_currency())
    if value_dist[asset] == None:
        return 0
    portfolio_value_percents = value_dist.normalize()

    diff_percent = percent - portfolio_value_percents[asset]

    percents = portfolio_value_percents
    percents[asset] += diff_percent
    percents[base_currency] -= diff_percent

    order_target_percents(percents,
                          order_type,
                          portfolio_getter,
                          price_getter,
                          place_order_executor)


def _order_target_percents(percents,
                          order_type,
                          portfolio_getter: callable,
                          price_getter: callable,
                          place_order_executor: callable):
    # TODO: Use Trade volume and Orderbook to limit the amount
    if order_type != OrderType.MARKET:
        raise ValueError(f'Unsupported order type {order_type} for '
                         f'order_target_percents.')

    # in case percent is negative zero
    for currency in percents.keys():
        if isclose(percents[currency], 0.0):
            percents[currency] = 0.0

    target_value_percents = Dist(percents)
    if any([v < 0 for k, v in target_value_percents.items()]):
        raise ValueError(
            f'negative order_target_percents request {target_value_percents}')
    # target_value_percents = target_value_percents.normalize()

    portfolio = portfolio_getter()
    logging.info('portfolio: %s', portfolio)

    value_dist = (
        portfolio.get_value_distribution_in_base_currency())
    portfolio_value_percents = value_dist.normalize()
    delta_value_percents = NormDist(
        target_value_percents - portfolio_value_percents)

    market_value = sum(value_dist.values())

    logging.info(f'market value {market_value}')
    logging.info(
        f'portfolio {portfolio_value_percents} (in {portfolio.base_currency})')

    logging.info(f'delta percents {delta_value_percents}')

    delta_values = delta_value_percents * market_value

    # Sort orders by e.g. USD amount, sell first.
    orders_in_base_currency = sorted(
        delta_values.items(), key=lambda x: x[1])

    logging.info(
        'pre-calulcated to-be-executed order pairs (in base currency) %s',
        orders_in_base_currency)

    for order_pair in orders_in_base_currency:
        if order_pair[0] != portfolio.base_currency:
            asset = order_pair[0]
            value_diff = order_pair[1]
            price = price_getter(asset)
            delta_amount = value_diff / price

            place_order_executor(MarketOrder(asset, delta_amount))


def order_target_percents(percents,
                          order_type,
                          portfolio_getter: callable,
                          price_getter: callable,
                          place_order_executor: callable):
    # TODO: Use Trade volume and Orderbook to limit the amount
    if order_type != OrderType.MARKET:
        raise ValueError(f'Unsupported order type {order_type} for '
                         f'order_target_percents.')

    # in case percent is negative zero
    for currency in percents.keys():
        if isclose(percents[currency], 0.0):
            percents[currency] = 0.0

    target_value_percents = Dist(percents)
    if any([v < 0 for k, v in target_value_percents.items()]):
        raise ValueError(
            f'negative order_target_percents request {target_value_percents}')

    portfolio = portfolio_getter()
    logging.info('portfolio: %s', portfolio)

    value_dist = (
        portfolio.get_value_distribution_in_base_currency())
    portfolio_value_percents = value_dist.normalize()
    delta_value_percents = NormDist(
        target_value_percents - portfolio_value_percents)

    if portfolio.base_currency in delta_value_percents.keys():
        delta_value_percents.pop(portfolio.base_currency)
    market_value = sum(value_dist.values())

    logging.info(f'market value {market_value}')
    logging.info(
        f'portfolio {portfolio_value_percents} (in {portfolio.base_currency})')

    logging.info(f'delta percents {delta_value_percents}')

    delta_values = delta_value_percents * market_value

    # Sort orders by e.g. USD amount, sell first.
    orders_in_base_currency = sorted(
        delta_values.items(), key=lambda x: x[1])

    logging.info(
        'pre-calulcated to-be-executed order pairs (in base currency) %s',
        orders_in_base_currency)

    for order_pair in orders_in_base_currency:
        if order_pair[0] != portfolio.base_currency:
            asset = order_pair[0]
            value_diff = order_pair[1]
            delta_amount = price_getter(asset, value_diff)
            place_order_executor(MarketOrder(asset, delta_amount))


def main(argv):
    del argv


if __name__ == '__main__':
    app.run(main)
