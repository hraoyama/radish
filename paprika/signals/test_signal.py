from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from absl import app, logging
import math
import sys, os


from paprika.data.fetcher import HistoricalDataFetcher, DataUploader
from paprika.data.feed_subscription import FeedSubscription
from paprika.data.feed_filter import Filtration

# from paprika.core.algorithm import Algorithm


class MovingAverageCross(Algorithm):
    def __init__(self):
        self.count = 0

    def register_feed(feed: FeedSubscription, filter: Filtration):
        pass
#
#     def handle_timer(self, event):
#         portfolio = get_portfolio(source)
#         markets = get_markets(source)
#         tickers = get_tickers(source)
#         for ticker in tickers.keys():
#             trades = get_my_trades(source, ticker)
#             orderbook = get_orderbook(source, ticker, depth=3)
#             open_orders = get_open_orders(source, ticker)
#             if len(open_orders):
#                 for open_order in open_orders:
#                     cancel_order(source, open_order)
#             timestamps, prices = get_ohlcv(
#                 source=source,
#                 symbol=ticker,
#                 frequency=frequency,
#                 fields=['close'],
#                 limit=5)
#         if self.count % 2 == 1:
#             percents = {}
#             for ticker in portfolio.avail_balance.keys():
#                 percents[ticker] = 0
#             order_target_percents(source, percents)
#         else:
#             # percents = {'ETF.IE00B8NB3063': 0.1}
#             percents = {}
#             for ticker in tickers:
#                 percents[ticker] = 0.5
#                 if math.isclose(sum(percents.values()), 1.0):
#                     break
#             order_target_percents(source, percents)
#
#         self.count += 1
#
#
# def main(argv):
#     # Backtest config
#     start = datetime(2018, 1, 29)
#     end = datetime(2018, 2, 24)
#     start_fund = 2e5
#     config = BacktestConfig(start_datetime=start, end_datetime=end, frequency=frequency)
#
#     # config.frequency = frequency
#     config.add_initial_portfolio(
#         source=source,
#         asset=asset,
#         amount=start_fund,
#         account_type=AccountType.EXCHANGE,
#         base_currency=currency)
#
#     # Run
#     with setup_runner(MovingAverageCross(), RunMode.BACKTEST, config) as runner:
#         result = runner.run()
#
#     logging.info(result.analysis)
#
#     # Plot algo EOD (end of day) market values
#     result.market_values.name = 'Portfolio'
#     result.market_values.plot(legend=True)
#
#     usd_eod = pd.Series(np.nan, index=result.portfolios.index, name=currency)
#     for index, portfolio in result.portfolios.iteritems():
#         usd_eod.loc[index] = portfolio.avail_balance[currency]
#     usd_eod.plot(legend=True)
#     # fetcher = HistoricalDataFetcher()
#     # benchmark = fetcher.fetch_ohlcv(
#     #     source, asset, frequency, start_time=start, end_time=end)['open']
#
#
#     # (benchmark * start_fund).plot(legend=True)
#
#     sns.set()
#     plt.show()
#     pass


# if __name__ == '__main__':
#     app.run(main)
#
#
#     # # checking redis connection etc.
#     # import redis
#     # r = redis.Redis(‘localhost’)
#     # r.keys()
#     # import arctic
#     # store = arctic.Arctic(‘localhost’)
#     # lib = store[‘mdb’]
#     # lib.list_symbols()
#     # df = lib.read('MTA.IT0005278236.Trade')
#     # r.set('MTA.IT0005278236.Trade', df.to_msgpack(compress='blosc'))
#
#     # 'MTA.IT0000062072.OrderBook'