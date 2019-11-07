from paprika.portfolio.portfolio import Portfolio
import copy
from paprika.utils.distribution import Dist
from paprika.utils.types import float_type
from paprika.utils.utils import isclose
from datetime import datetime


def test_portfolio():
    base_currency = 'EUR'
    balance = {base_currency: 100, 'ETF.DE0002635299': 10}
    p1 = Portfolio("Total", base_currency, balance)
    timestamp = datetime(2019, 2, 6, 12, 30)
    p1.portfolio_value(timestamp)
    p2 = Portfolio("S1", base_currency, balance)
    p4 = Portfolio("S2", base_currency, balance)
    p3 = Portfolio("S1S1", base_currency, balance)
    p2.add_sub_portfolio(p3)
    p1.add_sub_portfolio(p2)
    p1.add_sub_portfolio(p4)
    print(p1.portfolio_value(timestamp))
