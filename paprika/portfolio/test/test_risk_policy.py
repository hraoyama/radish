from paprika.portfolio.risk_policy import RiskPolicy
from paprika.portfolio.portfolio import Portfolio


def test_risk_policy():
    risk_policy = RiskPolicy()
    base_currency = 'EUR'
    balance = {base_currency: 100, 'ETF.DE0002635299': 10}
    p1 = Portfolio("Total", base_currency, balance)
    p_names = [f'sp{i+1}' for i in range(10)]
    p1 = risk_policy.allocate(p1, p_names)
    print(p1.list_sub_portfolio())
    sp_list = p1.list_sub_portfolio()
