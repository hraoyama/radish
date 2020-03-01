from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.feed import Feed
from paprika.utils import utils
from paprika.signals.signals.buy_on_gap import BuyOnGap

import numpy as np


def test_buy_on_gap_signal():
    
    DataType.extend("OPEN_PRICE")
    DataType.extend("BUY_PRICE")
    DataType.extend("RETURN_GAP")
    DataType.extend("CLOSE_MA")
    
    top_n = 10  # Max number of positions

    gap_feed = Feed('GAP_FEED', datetime(2000, 7, 1), datetime(2020, 1, 1))
    gap_feed.set_feed("SP_OPEN", DataType.OPEN_PRICE)
    gap_feed.set_feed("SP_BUY", DataType.BUY_PRICE)
    gap_feed.set_feed("SP_RETURN_GAP", DataType.RETURN_GAP)
    gap_feed.set_feed("SP_CLOSE_MA", DataType.CLOSE_MA)

    gap_signal = BuyOnGap(TOPN=top_n)
    gap_feed.add_subscriber(gap_signal)

    gap_signal.run()
    print(gap_signal.positions.head())
    positions = gap_signal.positions.values

    op = DataChannel.download("SP_OPEN.OPEN_PRICE")
    cl = DataChannel.download("SP_CLOSE.CLOSE_PRICE")

    returns_open_2_close = (cl - op) / op

    pnl = np.sum(positions * returns_open_2_close, axis=1)  # daily P&L of the strategy
    ret = pnl / top_n
    ret[ret.isnull()] = 0
    utils.stats_print(cl.index, ret)

    cost_per_transaction = 0.0005
    return_minus_costs = ret - utils.simple_transaction_costs(positions / top_n, cost_per_transaction)
    print("APR and Sharpe ratio after adjusting for transaction costs:")
    utils.stats_print(cl.index, return_minus_costs)

    # clear out any feeds in the cache
    DataChannel.clear_all_feeds()
