import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.feed import Feed
from paprika.signals.buy_on_gap_signal import BuyOnGap
from paprika.utils import utils

PATH = r'../../../resources/data/'


def main():

    topN = 10  # Max number of positions
    entryZscore = 1
    lookback = 20  # for MA

    op = pd.read_csv(os.path.join(PATH, 'inputDataOHLCDaily_20120424_op.csv'))
    lo = pd.read_csv(os.path.join(PATH, 'inputDataOHLCDaily_20120424_lo.csv'))
    cl = pd.read_csv(os.path.join(PATH, 'inputDataOHLCDaily_20120424_cl.csv'))
    stocks = pd.read_csv(os.path.join(PATH, 'inputDataOHLCDaily_20120424_stocks.csv'))

    op['Var1'] = pd.to_datetime(op['Var1'], format='%Y%m%d')
    op.columns = np.insert(stocks.values, 0, 'date')
    op.set_index('date', inplace=True)

    lo['Var1'] = pd.to_datetime(lo['Var1'], format='%Y%m%d')
    lo.columns = np.insert(stocks.values, 0, 'date')
    lo.set_index('date', inplace=True)

    cl['Var1'] = pd.to_datetime(cl['Var1'], format='%Y%m%d')
    cl.columns = np.insert(stocks.values, 0, 'date')
    cl.set_index('date', inplace=True)

    # standard deviation is computed using close returns for the last 90 days
    vol_estimate = utils.returns_calculator(cl, 1).rolling(90).std().shift(1)
    # select stocks near the market open whose returns from their previous day's low to today's open are lower
    # than one standard deviation
    buy_price = lo.shift(1) * (1 + entryZscore * vol_estimate)
    ret_gap = (op - lo.shift(1)) / lo.shift(1)
    ma = cl.shift(1).rolling(lookback).mean()

    DataType.extend("OPEN_PRICE")
    table_name1 = DataChannel.name_to_data_type("SP_OPEN", DataType.OPEN_PRICE)
    DataChannel.upload(op, table_name1)

    DataType.extend("BUY_PRICE")
    table_name2 = DataChannel.name_to_data_type("SP_BUY", DataType.BUY_PRICE)
    DataChannel.upload(buy_price, table_name2)

    DataType.extend("RETURN_GAP")
    table_name3 = DataChannel.name_to_data_type("SP_RETURN_GAP", DataType.RETURN_GAP)
    DataChannel.upload(ret_gap, table_name3)

    DataType.extend("CLOSE_MA")
    table_name3 = DataChannel.name_to_data_type("SP_CLOSE_MA", DataType.CLOSE_MA)
    DataChannel.upload(ma, table_name3)

    gap_feed = Feed('GAP_FEED', datetime(2000, 7, 1), datetime(2020, 1, 1))
    gap_feed.set_feed(["SP_OPEN"], DataType.OPEN_PRICE)
    gap_feed.set_feed(["SP_BUY"], DataType.BUY_PRICE)
    gap_feed.set_feed(["SP_RETURN_GAP"], DataType.RETURN_GAP)
    gap_feed.set_feed(["SP_CLOSE_MA"], DataType.CLOSE_MA)

    gap_signal = BuyOnGap(TOPN=topN)
    gap_feed.add_subscriber(gap_signal)

    gap_signal.run()
    print(gap_signal.positions.head())
    positions = gap_signal.positions.values

    returns_open_2_close = (cl - op) / op

    pnl = np.sum(positions * returns_open_2_close, axis=1)  # daily P&L of the strategy
    ret = pnl / topN
    ret[ret.isnull()] = 0
    utils.stats_print(cl.index, ret)

    cost_per_transaction = 0.0005
    return_minus_costs = ret - utils.simple_transaction_costs(positions / topN, cost_per_transaction)
    print("APR and Sharpe ratio after adjusting for transaction costs:")
    utils.stats_print(cl.index, return_minus_costs)

    # clear out any feeds in the cache
    DataChannel.clear_all_feeds()


if __name__ == "__main__":
    main()
