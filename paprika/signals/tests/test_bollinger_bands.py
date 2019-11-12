from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.signals.assessor import Assessor
from paprika.signals.signal_bollinger_bands import BollingerBands
from paprika.utils import utils
import matplotlib.pyplot as plt


def test_bollinger_bands():

    # # in order to explore the data in advance
    # from paprika.data.data_channel import DataChannel
    # DataChannel.check_register(["GLD","GDX"], feeds_db=False)
    # gld = DataChannel.download('GLD.OHLCVAC_PRICE', DataChannel.PERMANENT_ARCTIC_SOURCE_NAME, use_redis=False )
    # gdx = DataChannel.download('GDX.OHLCVAC_PRICE', DataChannel.PERMANENT_ARCTIC_SOURCE_NAME, use_redis=False )
    # gld.index.intersection(gdx.index)
    # gdx.index.intersection(gld.index)

    tickers = ["GLD", "GDX"]
    bollinger_feed = Feed('GOLD_FEED', datetime(1950, 7, 1), datetime(2050, 1, 1))
    bollinger_feed.set_feed(tickers, DataType.OHLCVAC_PRICE, how='inner')

    gold_bollinger = BollingerBands(LOOKBACK=20, Y_NAME="GLD", X_NAME="GDX")
    bollinger_feed.add_subscriber(gold_bollinger)

    gold_bollinger.run()
    print(gold_bollinger.positions.head())
    gold_signal_data = gold_bollinger.signal_data()
    gold_assessor = Assessor(gold_signal_data)

