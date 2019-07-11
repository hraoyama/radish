from abc import ABC

class BaseConfig(ABC):
    def __init__(self):
        self.mongodb_host = 'localhost'
        self.redis_host = 'localhost'
        self.initial_portfolios = {}

    def add_initial_portfolio(self,
                              source: str,
                              asset: str = 'USD',
                              amount: float = 1000.0,
                              account_type: str = None,
                              base_currency: str = 'USD'):
        from paprika.portfolio.account_type import AccountType
        from paprika.portfolio.portfolio import Portfolio

        if account_type is None:
            account_type = AccountType.EXCHANGE

        if account_type is AccountType.EXCHANGE:
            portfolio = Portfolio(
                source,
                base_currency=base_currency,
                balance={
                    asset: amount
                })
        else:
            raise NotImplementedError
        if not (source, account_type) in self.initial_portfolios:
            self.initial_portfolios[(source, account_type)] = portfolio
        else:
            self.initial_portfolios[(source, account_type)] += portfolio


class BacktestConfig(BaseConfig):
    def __init__(self, start_datetime, end_datetime, frequency):
        super().__init__()
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.frequency = frequency

        from paprika.data.marketdata import PriceSource
        self.price_source = {'source': PriceSource.OHLCV, 'frequency': self.frequency}
        self.transaction_fee = 0.0025
