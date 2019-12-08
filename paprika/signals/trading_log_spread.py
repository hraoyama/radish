# Trading Log Price Spread
from paprika.core.base_signal import Signal
from paprika.data.fetcher import DataType
from paprika.data.feed_subscriber import FeedSubscriber

import numpy as np
import pandas as pd
import re
from typing import List, Tuple
import logging

from paprika.signals.signal_data import SignalData
from sklearn.linear_model import LinearRegression


class DynamicLogSpread(FeedSubscriber, Signal):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.y_name = str(self.get_parameter("Y_NAME")).upper().strip()
        self.x_name = str(self.get_parameter("X_NAME")).upper().strip()
        self.y_name_m = re.compile("^" + self.y_name + ".*")
        self.x_name_m = re.compile("^" + self.x_name + ".*")
        self.lookback = self.get_parameter("LOOKBACK")
        self.positions = pd.DataFrame()
        self.prices = pd.DataFrame()
        self.spreads = []
        # if either trades come in at separate times or close prices come in a the same time
        # we should be able to handle either
        self.y_data = None
        self.x_data = None
        self.max_time_distance = self.get_parameter("MAX_TIME_DISTANCE", pd.Timedelta(np.timedelta64(10, 's')))

    def calc_beta(self):
        regress_results = LinearRegression().fit(np.log(self.prices.iloc[-self.lookback:][self.x_name].values.reshape(-1, 1)),
                                                 np.log(self.prices.iloc[-self.lookback:][self.y_name].values))
        return regress_results.coef_[0]

    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):

        super().handle_event(events)
        if len(events) == 1:
            event = events[0]
            price_column = 'Price' if event[0] == DataType.TRADES else 'Adj Close'
            data1 = event[1]
            y_data = data1[[self.y_name_m.match(x) is not None for x in data1['Symbol']]]
            x_data = data1[[self.x_name_m.match(x) is not None for x in data1['Symbol']]]
            execute = False
            last_index = None
            if event[0] == DataType.TRADES:
                if y_data.shape[0] > 0:
                    if self.y_data is None and self.x_data is not None:
                        if (y_data.index[0] - self.x_data.index[0]) > self.max_time_distance:
                            self.x_data = None
                        else:
                            execute = True
                    self.y_data = y_data
                    last_index = self.y_data.index[-1]
                if x_data.shape[0] > 0:
                    if self.x_data is None and self.y_data is not None:
                        if (x_data.index[0] - self.y_data.index[0]) > self.max_time_distance:
                            self.y_data = None
                        else:
                            execute = True
                    self.x_data = x_data
                    last_index = self.x_data.index[-1]
            else:
                execute = True
                self.y_data = y_data
                self.x_data = x_data
                if len(self.y_data.index) < 1:
                    print(str(self.y_data))

                last_index = self.y_data.index[-1]

            if execute:
                self.prices = self.prices.append(
                    pd.DataFrame({'DateTime': [last_index],
                                  self.y_name: self.y_data[price_column][-1],
                                  self.x_name: self.x_data[price_column][-1]}))

                if len(self.prices) < self.lookback:
                    self.spreads.append(np.nan)
                    self.positions = self.positions.append(
                        pd.DataFrame({'DateTime': [last_index],
                                      self.y_name: [np.nan],
                                      self.x_name: [np.nan]}))
                else:
                    beta = self.calc_beta()
                    spread = np.log(self.prices[self.y_name][-1:].values[0]) - beta * np.log(self.prices[self.x_name][-1:].values[0])
                    self.spreads.append(spread)

                    num_units = -(spread - np.nanmean(self.spreads[-self.lookback:])) / \
                                   np.nanstd(self.spreads[-self.lookback:], ddof=1)
                    self.positions = self.positions.append(
                        pd.DataFrame({'DateTime': [last_index],
                                      self.y_name: [num_units],
                                      self.x_name: [-num_units * beta]}))

                    logging.info(f'{self.positions}')
                    self.y_data = None
                    self.x_data = None
        else:
            logging.info(f'There are {len(events)} events.')

    def signal_data(self):
        return SignalData(self.__class__.__name__,
                          [("positions", SignalData.create_indexed_frame(
                              self.positions[[self.y_name, self.x_name]].fillna(method='ffill'))),
                           ("prices", SignalData.create_indexed_frame(self.prices)),
                           ("spreads", SignalData.create_indexed_frame(self.spreads))])
