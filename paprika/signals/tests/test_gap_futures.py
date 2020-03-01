from datetime import datetime

from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.signals.signals.gap_futures import GapFutures
from paprika.utils import utils


def test_gap_futures():
    DataChannel.clear_redis()

    tickers = ['FSTX']
    my_feed = Feed('Gap_Futures', datetime(2000, 7, 1), datetime(2020, 1, 1))
    my_feed.set_feed(tickers, DataType.OHLCVAC_PRICE, how='inner')

    my_signal = GapFutures(zscore=0.1, look_back=90)
    my_feed.add_subscriber(my_signal)
    my_signal.run()

    my_positions, my_prices = my_signal.positions, my_signal.prices
    my_returns = (my_prices['Close'] - my_prices['Open']) / my_prices['Open']
    # daily P&L of the strategy
    my_pnl = my_positions * my_returns
    my_pnl[my_pnl.isnull()] = 0
    _ = utils.stats_print(my_prices['DateTime'], my_pnl)
