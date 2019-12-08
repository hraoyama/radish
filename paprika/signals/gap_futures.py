# Example 7.1: Opening Gap Strategy for Dow Jones STOXX 50 index futures (FSTX) trading on Eurex
# This one is not profitable, but can be used as one of the signals
# Mean reversion works though

from paprika.data.fetcher import DataType
from paprika.data.feed_subscriber import FeedSubscriber
from typing import List, Tuple
from paprika.signals.signal_data import SignalData

import numpy as np
import pandas as pd
import logging


class GapFutures(FeedSubscriber):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.zscore = self.get_parameter("zscore")
        self.ticker = self.get_parameter("ticker")
        self.look_back = self.get_parameter("look_back")
        self.prices = pd.DataFrame()
        self.returns = []
        self.positions = []

    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):

        super().handle_event(events)
        if len(events) == 1:
            event = events[0]
            data1 = event[1]
            last_index = data1.index[0]

            temp_dct = {'DateTime': [last_index],
                        'Open': data1['Open'].values[0],
                        'High': data1['High'].values[0],
                        'Low': data1['Low'].values[0],
                        'Close': data1['Close'].values[0]}

            self.prices = self.prices.append(pd.DataFrame(temp_dct))
            if self.prices.shape[0] == 1:
                self.returns.append(np.nan)
            else:
                self.returns.append(self.prices['Close'].values[-1] / self.prices['Close'].values[-2] - 1)

            if len(self.prices) < self.look_back:
                self.positions.append(0)
            else:
                vol = np.std(np.array(self.returns[-self.look_back:]), ddof=1)
                op = temp_dct['Open']

                if op >= self.prices['High'].values[-2] * (1 + self.zscore * vol):
                    self.positions.append(-1)
                elif op <= self.prices['Low'].values[-2] * (1 - self.zscore * vol):
                    self.positions.append(1)
                else:
                    self.positions.append(0)

                logging.info(f'{self.positions}')
        else:
            logging.info(f'There are {len(events)} events.')

    def signal_data(self):
        return SignalData(self.__class__.__name__,
                          [("positions", SignalData.create_indexed_frame(self.positions)),
                           ("prices", SignalData.create_indexed_frame(self.prices))])
