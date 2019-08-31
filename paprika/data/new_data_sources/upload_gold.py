import pandas as pd
import os
import matplotlib.pyplot as plt

from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.feed import Feed
from paprika.data.feed_filter import TimeFreqFilter, Filtration, TimePeriod
from paprika.signals.gold_cointegration import GoldSpread
from paprika.utils import utils

PATH = r'../../../resources/data/'


def main():
    tckr1 = 'GLD'
    tckr2 = 'GDX'
    ts1 = pd.read_excel(os.path.join(PATH, '{}.xls'.format(tckr1)), usecols=[0, 6])
    ts1 = ts1.sort_values(by=['Date'])
    ts1 = ts1.reset_index(drop=True)

    ts2 = pd.read_excel(os.path.join(PATH, '{}.xls'.format(tckr2)), usecols=[0, 6])
    ts2 = ts2.sort_values(by=['Date'])
    ts2 = ts2.reset_index(drop=True)

    ts = pd.merge(ts1, ts2, how='inner', on=['Date'])
    ts.columns = ['date', 'GLD', 'GDX']
    ts.set_index('date', inplace=True)
    
    DataType.extend("EOD_PRICE")
    
    table_name = DataChannel.name_to_data_type("GOLD_GDX", DataType.EOD_PRICE)
    DataChannel.upload(ts, table_name)
    # following stores it in DB permanently - only do this if you are sure you need to keep this data
    # DataChannel.upload_to_permanent(table_name)

    gold_feed = Feed('GOLD_FEED', datetime(1950, 7, 1), datetime(2050, 1, 1))
    gold_feed.set_feed("GOLD_GDX", DataType.EOD_PRICE)

    gold_signal = GoldSpread(BETA=1.631, MEAN=0.052196, STD=1.9487, ENTRY=1, EXIT=0.5)
    gold_feed.add_subscriber(gold_signal)

    gold_signal.run()
    print(gold_signal.positions.head())
    positions = gold_signal.positions[['GLD', 'GDX']].fillna(method='ffill').values

    train_idx = 252  # window where above parameters were estimated

    returns = utils.returns_calculator(ts[['GLD', 'GDX']], 1)
    port_return = utils.portfolio_return_calculator(positions, returns)
    plt.plot(ts.index, port_return.cumsum())
    plt.xticks(rotation=45)
    plt.ylabel('Cumulative return')
    plt.title("Cointegration of {} vs. {}.".format(tckr1, tckr2))
    plt.show()

    sharpe_tr = utils.sharpe(port_return[:train_idx], 252)
    sharpe_test = utils.sharpe(port_return[train_idx:], 252)
    print("Sharpe ratio on the train {:.2f} and test {:.2f} sets respectively.".format(sharpe_tr, sharpe_test))

    cost_per_transaction = 0.0005
    port_return_minus_costs = port_return - utils.simple_transaction_costs(positions, cost_per_transaction)
    sharp_cost_adj_tr = utils.sharpe(port_return_minus_costs[:train_idx], 252)
    sharp_cost_adj_test = utils.sharpe(port_return_minus_costs[train_idx:], 252)
    print("Sharpe ratio on the train {:.2f} and test {:.2f} sets respectively "
          "after adjusting for transaction costs.".format(sharp_cost_adj_tr, sharp_cost_adj_test))


if __name__ == "__main__":
    main()
