from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.signals.cointegration_commodity_ETFs import CointegrationTriplet
import numpy as np
from paprika.signals import utils


def test_signal_cointegration_triplet():

    tickers = ["EWA", "EWC", "IGE2"]
    triplet_feed = Feed('COMMODITY_SPREAD', datetime(2000, 7, 1), datetime(2020, 1, 1))
    triplet_feed.set_feed(tickers, DataType.OHLCVAC_PRICE, how='inner')
    etf_weights = np.array([0.7599635, -1.04602749,  0.22330592])
    half_life = 22.66257785048902

    triplet_signal = CointegrationTriplet(TICKERS=tickers, BETAS=etf_weights, HALF_LIFE=half_life)
    triplet_feed.add_subscriber(triplet_signal)
    triplet_signal.run()

    positions = triplet_signal.positions
    prices = triplet_signal.prices
    returns = utils.returns_calculator(prices[triplet_signal.tickers], 1)

    # daily P&L of the strategy
    pnl = utils.portfolio_return_calculator(positions[triplet_signal.tickers], returns)
    strategy_return = pnl / np.sum(np.abs(positions[triplet_signal.tickers].shift()), axis=1)
    strategy_return[strategy_return.isnull()] = 0
    _ = utils.stats_print(prices['DateTime'], strategy_return)
