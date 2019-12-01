"""
Cointegration strategy between triplet of ETFs (EWA, EWC, IGE)
"""

from paprika.data.fetcher import DataType
from paprika.data.feed_subscriber import FeedSubscriber
from typing import List, Tuple
from paprika.signals.signal_data import SignalData

import numpy as np
import pandas as pd
import logging


class CointegrationTriplet(FeedSubscriber):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.tickers = self.get_parameter("TICKERS")  # this has to be aligned with self.betas
        self.betas = self.get_parameter("BETAS")  # should come from research or could be computed on the fly
        self.half_life = self.get_parameter("HALF_LIFE")  # should come from research or could be computed on the fly
        self.look_back = int(self.half_life)
        self.unit_portfolios = []
        self.prices = pd.DataFrame()
        self.positions = pd.DataFrame()

    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):

        super().handle_event(events)
        if len(events) == 1:
            event = events[0]
            price_column = 'Price' if event[0] == DataType.TRADES else 'Adj Close'
            data1 = event[1]
            price_data = [data1[[ticker in symbol for symbol in data1['Symbol']]] for ticker in self.tickers]
            last_index = price_data[0].index[-1]

            temp_dct = {'DateTime': [last_index]}
            temp_dct.update({tick: [price_data[i][price_column][-1]] for i, tick in enumerate(self.tickers)})
            temp_df = pd.DataFrame(temp_dct)

            self.prices = self.prices.append(temp_df)
            unit_portfolio = np.dot(temp_df[self.tickers].values, self.betas)[0]

            if len(self.prices) < self.look_back:

                temp_dct2 = {'DateTime': [last_index]}
                temp_dct2.update({tick: [np.nan] for tick in self.tickers})
                temp_df2 = pd.DataFrame(temp_dct2)

                self.unit_portfolios.append(unit_portfolio)
                self.positions = self.positions.append(temp_df2)
            else:

                num_units = -(unit_portfolio - np.nanmean(self.unit_portfolios[-self.look_back:])) / \
                               np.nanstd(self.unit_portfolios[-self.look_back:], ddof=1)

                temp_dct2 = {'DateTime': [last_index]}
                temp_dct2.update({tick: [num_units*beta*temp_df[tick].values[0]]
                                  for tick, beta in zip(self.tickers, self.betas)})
                temp_df2 = pd.DataFrame(temp_dct2)

                self.positions = self.positions.append(temp_df2)
                logging.info(f'{self.positions}')
        else:
            logging.info(f'There are {len(events)} events.')

    def signal_data(self):
        return SignalData(self.__class__.__name__,
                          [("positions", SignalData.create_indexed_frame(
                              self.positions[[self.y_name, self.x_name]].fillna(method='ffill'))),
                           ("prices", SignalData.create_indexed_frame(self.prices)),
                           ("unit_portfolios", SignalData.create_indexed_frame(self.unit_portfolios))])
