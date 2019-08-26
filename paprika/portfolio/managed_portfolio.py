import pandas as pd

from paprika.portfolio.risk_policy import RiskPolicy
from paprika.utils.record import RecorderOffline


class ManagedPortfolio(object):
    def __init__(self, valuation_type, risk_policy: RiskPolicy):
        self.risk_policy = risk_policy
        self.executed_trades = None
        self.allocation = None
        self.price_observations = RecorderOffline(key_column='ISIN', value_column='Executed_Px')  # a default dict
        self.order_manager = None
        self.valuation_type = valuation_type
        pass
    
    def accept_execution(self, executed_order: pd.DataFrame):
        # add to executed trades
        # alter the allocation
        # apply the risk policy # should the order manager be inside the risk policy ?
        # send out risk reducer trades
        pass
    
    def accept_market(self, markets: pd.DataFrame):
        # add to limit_price observations
        pass
    
    @property
    def allocation(self):
        # return the allocation
        return 0
    
    @property
    def value(self, time_index=None):
        # call value on the allocation passing in the limit_price observations at the right time
        return 0.0


class Allocation(object):
    def accept_execution(self, executed_order: pd.DataFrame):
        pass