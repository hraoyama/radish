from paprika.portfolio.portfolio import Portfolio

from abc import ABC
from typing import List
from absl import logging


class RiskPolicy(ABC):
    def allocate(self,
                 whole_portfolio: Portfolio,
                 sub_portfolio_names: List[str],
                 how='Equally') -> Portfolio:
        if how == 'Equally':
            return self.equally_allocate(whole_portfolio,
                                         sub_portfolio_names)

    def equally_allocate(self,
                         whole_portfolio: Portfolio,
                         sub_portfolio_names: List[str]) -> Portfolio:
        if len(whole_portfolio.list_sub_portfolio()) > 0:
            logging.info(f'Portfolio {whole_portfolio.name} is not empty.'
                         f'Will merge all sub portfolios,'
                         f'then allocate them again.')
            whole_portfolio = self.merge_sub_portfolios(whole_portfolio)
        num = len(sub_portfolio_names)
        sub_balance = whole_portfolio.balance / num
        base_currency = whole_portfolio.base_currency
        for portfolio_name in sub_portfolio_names:
            portfolio = Portfolio(portfolio_name, base_currency, sub_balance)
            whole_portfolio.add_sub_portfolio(portfolio)
        return whole_portfolio

    @staticmethod
    def merge_sub_portfolios(portfolio: Portfolio) -> Portfolio:
        balance = portfolio.balance
        portfolio.clear_sub_portfolio()
        portfolio.balance = balance
        return portfolio
