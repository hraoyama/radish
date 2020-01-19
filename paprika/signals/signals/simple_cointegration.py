from paprika.core.base_signal import Signal
from paprika.data.fetcher import DataType
from paprika.data.feed_subscriber import FeedSubscriber
from paprika.signals.signal_data import SignalData
from scipy.stats import norm

import numpy as np
import pandas as pd
import re
from typing import List, Tuple
import logging


class CointegrationSpread(FeedSubscriber, Signal):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.y_name = str(self.get_parameter("Y_NAME")).upper().strip()
        self.x_name = str(self.get_parameter("X_NAME")).upper().strip()
        self.y_name_m = re.compile("^" + self.y_name + ".*")
        self.x_name_m = re.compile("^" + self.x_name + ".*")
        self.beta = self.get_parameter("BETA")
        self.mean = self.get_parameter("MEAN")
        self.sigma = self.get_parameter("STD")
        self.entry = self.get_parameter("ENTRY")
        self.exit = self.get_parameter("EXIT")
        self.positions = pd.DataFrame()
        self.prices = pd.DataFrame()
        self.probabilities = pd.DataFrame()
        self.y_and_x_positions = []

        # if either trades come in at separate times or close prices come in a the same time
        # we should be able to handle either
        self.y_data = None
        self.x_data = None
        self.max_time_distance = self.get_parameter("MAX_TIME_DISTANCE", pd.Timedelta(np.timedelta64(10, 's')))

    def calc_long_short(self, z_score):

        if z_score <= -self.entry:
            y_pos, x_pos = 1., -1.

        elif z_score >= self.entry:
            y_pos, x_pos = -1., 1.

        elif np.abs(z_score) <= self.exit:
            y_pos, x_pos = 0., 0.
        else:
            y_pos, x_pos = np.nan, np.nan

        return y_pos, x_pos

    def calc_position(self, z_score, idx):

        y_pos, x_pos = self.calc_long_short(z_score)
        if not self.y_and_x_positions:
            y_pos_prev, x_pos_prev = (0, 0)
        else:
            y_pos_prev, x_pos_prev = self.y_and_x_positions[-1]

        if (y_pos is np.nan) | (x_pos is np.nan):
            y_pos = y_pos_prev
            x_pos = x_pos_prev

        self.y_and_x_positions.append((y_pos, x_pos))

        self.positions = self.positions.append(
            pd.DataFrame({'DateTime': [idx],
                          self.y_name: [y_pos],
                          self.x_name: [x_pos]}))

        logging.info(f'{self.positions}')

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
                last_index = self.y_data.index[-1]

            if execute:
                self.prices = self.prices.append(
                    pd.DataFrame({'DateTime': [last_index],
                                  self.y_name: self.y_data[price_column][-1],
                                  self.x_name: self.x_data[price_column][-1]}))
                z_score = (self.y_data[price_column][0] - self.beta * self.x_data[price_column][
                    0] - self.mean) / self.sigma
                self.calc_position(z_score, last_index)
                self.y_data = None
                self.x_data = None
        else:
            logging.info(f'There are {len(events)} events.')

    def signal_data(self):
        return SignalData(self.__class__.__name__,
                          [("positions", SignalData.create_indexed_frame(
                              self.positions[["DateTime",self.y_name, self.x_name]].fillna(method='ffill'))),
                           ("prices", SignalData.create_indexed_frame(self.prices)),
                           ("probabilities", SignalData.create_indexed_frame(self.probabilities))])
