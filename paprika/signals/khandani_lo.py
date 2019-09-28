import utils
import pandas as pd
import numpy as np
import seaborn as sn
import os
sn.set()


def main():

    ts = pd.read_table(os.path.join(utils.PATH, 'SPX_20071123.txt'))  # S&P 500 data
    # ts = pd.read_table(os.path.join(utils.PATH, 'IJR_20080114.txt'))  # S&P 600 small-cap stocks
    ts['Date'] = pd.to_datetime(ts['Date'].map(np.round), format='%Y%m%d')
    # do not forget to sort by date
    ts = ts.sort_values(by=['Date'])
    start_date = pd.to_datetime(20060101, format='%Y%m%d')
    end_date = pd.to_datetime(20061231, format='%Y%m%d')
    date_filter = (ts['Date'] >= start_date) & (ts['Date'] <= end_date)

    # do not use inbuilt pct_change() function. you can get unexpected behaviour
    returns = utils.returns_calculator(ts[ts.columns[1:]], 1)
    market = returns.mean(axis=1)
    weights = -(returns.T - market).T
    scaler = np.nansum(np.abs(weights), axis=1)
    scaler[scaler == 0] = 1
    weights = (weights.T / scaler).T

    port_return = utils.portfolio_return_calculator(weights, returns)
    sharp = utils.sharpe(port_return[date_filter], 252)
    print("Sharpe ratio on the selected period is {:.2f}.".format(sharp))

    cost_per_transaction = 0.0005
    port_return_minus_costs = port_return - utils.simple_transaction_costs(weights, cost_per_transaction)
    sharp_cost_adj = utils.sharpe(port_return_minus_costs[date_filter], 252)
    print("Sharpe ratio on the selected period after adjusting for transaction costs is {:.2f}.".format(sharp_cost_adj))


if __name__ == "__main__":
    main()
