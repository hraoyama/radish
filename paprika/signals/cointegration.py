import utils
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sn
import os
from sklearn.linear_model import LinearRegression
sn.set()


def main(tckr1, tckr2):
    ts1 = pd.read_excel(os.path.join(utils.PATH, '{}.xls'.format(tckr1)), usecols=[0, 6])
    ts1 = ts1.sort_values(by=['Date'])
    ts1 = ts1.reset_index(drop=True)

    ts2 = pd.read_excel(os.path.join(utils.PATH, '{}.xls'.format(tckr2)), usecols=[0, 6])
    ts2 = ts2.sort_values(by=['Date'])
    ts2 = ts2.reset_index(drop=True)

    ts = pd.merge(ts2, ts1, how='inner', on=['Date'])
    train_idx = 252
    X = ts['Adj Close_x'][:train_idx].values.reshape(-1, 1)
    y = ts['Adj Close_y'][:train_idx].values

    # Let's check that it is cointegrated with Engle-Granger:
    # Should we do it on the whole set or just train?
    _, p_val, _ = utils.cadf_test(ts['Adj Close_y'], ts['Adj Close_x'])
    print("P-value of cointegration test between {} and {} on the whole set is {:.2f}".format(tckr1, tckr2, p_val))
    _, p_val, _ = utils.cadf_test(y, X)
    print("P-value of cointegration test between {} and {} on the train set is {:.2f}".format(tckr1, tckr2, p_val))

    # Ernie does not use intercept and he gets much better Sharpe with it
    lm = LinearRegression(fit_intercept=False).fit(X, y)
    beta = lm.coef_[0]

    spread = ts['Adj Close_y'] - beta * ts['Adj Close_x']
    plt.plot(ts['Date'], spread)
    plt.xticks(rotation=45)
    plt.title('Spread between GLD and GDX')
    plt.show()

    mu_train = spread[:train_idx].mean()
    vol_train = spread[:train_idx].std()
    z_scores = (spread - mu_train) / vol_train
    half_life = utils.half_life(z_scores)
    print('Half-life for mean reversion is {:.2f}'.format(half_life))

    longs = z_scores <= -1
    shorts = z_scores >= 1
    exits = np.abs(z_scores) <= 0.5
    N = len(z_scores)

    positions = np.full((N, 2), np.nan)

    positions[longs, :] = [1, -1]
    positions[shorts, :] = [-1, 1]
    positions[exits, :] = [0, 0]
    positions = pd.DataFrame(positions).fillna(method='ffill').values
    T = 252

    ts['ret_{}'.format(tckr1)] = utils.returns_calculator(ts['Adj Close_y'], 1)
    ts['ret_{}'.format(tckr2)] = utils.returns_calculator(ts['Adj Close_x'], 1)
    corr_val, corr_pval = utils.correlation_test(ts['ret_{}'.format(tckr1)], ts['ret_{}'.format(tckr2)])
    print("Correlation between {} and {} is {:.2f} with the respective p-value {:.3f}".format(tckr1, tckr2, corr_val, corr_pval))

    port_return = utils.portfolio_return_calculator(positions, ts[['ret_{}'.format(tckr1), 'ret_{}'.format(tckr2)]])
    plt.plot(ts['Date'], port_return.cumsum())
    plt.xticks(rotation=45)
    plt.ylabel('Cumulative return')
    plt.title("Cointegration of {} vs. {}.".format(tckr1, tckr2))
    plt.show()

    sharpe_tr = utils.sharpe(port_return[:train_idx], T)
    sharpe_test = utils.sharpe(port_return[train_idx:], T)
    print("Sharpe ratio on the train {:.2f} and test {:.2f} sets respectively.".format(sharpe_tr, sharpe_test))

    cost_per_transaction = 0.0005
    port_return_minus_costs = port_return - utils.simple_transaction_costs(positions, cost_per_transaction)
    sharp_cost_adj_tr = utils.sharpe(port_return_minus_costs[:train_idx], T)
    sharp_cost_adj_test = utils.sharpe(port_return_minus_costs[train_idx:], T)
    print("Sharpe ratio on the train {:.2f} and test {:.2f} sets respectively after adjusting for transaction costs.".format(sharp_cost_adj_tr, sharp_cost_adj_test))


if __name__ == "__main__":
    # tickers to test for cointegration
    tck1, tck2 = 'GLD', 'GDX'
    main(tck1, tck2)
    # tck1, tck2 = 'KO', 'PEP'
    # main(tck1, tck2)


