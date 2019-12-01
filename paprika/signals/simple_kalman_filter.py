"""
Mean Reversion Strategy on EWA and EWC ETF pairs using Kalman Filter.
Need to move to proper implementation. Also have to check the dimensionality of the matrices (i.e., unit tests)
"""

from paprika.core.base_signal import Signal
from paprika.data.fetcher import DataType
from paprika.data.feed_subscriber import FeedSubscriber
from paprika.signals.signal_data import SignalData

import re
from typing import List, Tuple
import logging
import numpy as np
import pandas as pd


class SimpleKalmanSignal(FeedSubscriber, Signal):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.y_name = str(self.get_parameter("Y_NAME")).upper().strip()
        self.x_name = str(self.get_parameter("X_NAME")).upper().strip()
        self.y_name_m = re.compile("^" + self.y_name + ".*")
        self.x_name_m = re.compile("^" + self.x_name + ".*")

        # this has to be changed to any number
        self.param_dim = 2
        self.delta = self.get_parameter("delta")
        self.Vw = self.delta / (1 - self.delta) * np.eye(self.param_dim)
        self.Ve = self.get_parameter("Ve")

        self.beta = [[0, 0]]  # Initialize to zero
        # For clarity, we denote R(t|t) by P(t). Initialize R, P and beta.
        self.R = np.zeros((self.param_dim, self.param_dim))  # variance-covariance matrix of beta: R(t|t-1)
        self.P = self.R.copy()  # variance-covariance matrix of beta: R(t|t)
        self.counter = 0

        self.num_units_long = pd.DataFrame()
        self.num_units_short = pd.DataFrame()

        self.positions = pd.DataFrame()
        self.prices = pd.DataFrame()
        # if either trades come in at separate times or close prices come in a the same time
        # we should be able to handle either
        self.y_data = None
        self.x_data = None
        self.max_time_distance = self.get_parameter("MAX_TIME_DISTANCE", pd.Timedelta(np.timedelta64(10, 's')))

    def calc_kalman(self):

        t = self.counter
        if t > 0:
            self.beta.append(self.beta[t - 1])
            self.R = self.P + self.Vw

        x_vec = [self.x_data['Adj Close'].values[0], 1]
        y_hat = np.dot(x_vec, self.beta[t])
        Q = np.dot(x_vec, np.dot(self.R, x_vec)) + self.Ve
        e = self.y_data['Adj Close'].values[0] - y_hat  # measurement prediction error
        K = np.dot(x_vec, self.R) / Q  # Kalman gain
        self.beta[t] = list(np.array(self.beta[t]) + np.dot(K, e))  # State update. Equation 3.11
        # State covariance update. Equation 3.12
        self.P = self.R - np.dot(np.dot(K.reshape(-1, 1), np.array(x_vec).reshape(-1, 1).T), self.R)
        self.counter += 1

        return self.beta, e, Q

    def calc_position(self, e, Q):

        long_entry = e < -np.sqrt(Q)
        long_exit = e > 0

        short_entry = e > np.sqrt(Q)
        short_exit = e < 0

        num_unit_long = np.nan
        num_unit_short = np.nan
        if self.counter == 0:
            num_unit_long = 0
            num_unit_short = 0

        if long_entry:
            num_unit_long = 1
        if long_exit:
            num_unit_long = 0

        self.num_units_long = self.num_units_long.append(pd.DataFrame([num_unit_long]))
        self.num_units_long.fillna(method='ffill', inplace=True)

        if short_entry:
            num_unit_short = -1
        if short_exit:
            num_unit_short = 0

        self.num_units_short = self.num_units_short.append(pd.DataFrame([num_unit_short]))
        self.num_units_short.fillna(method='ffill', inplace=True)
        num_units = self.num_units_long + self.num_units_short

        t = self.counter - 1
        positions = num_units.iloc[t].values[0] * np.array([-self.beta[t][0], 1]) * np.array([self.x_data['Adj Close'].values[0], self.y_data['Adj Close'].values[0]])
        return positions

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

                beta, e, Q = self.calc_kalman()
                positions = self.calc_position(e, Q)

                self.positions = self.positions.append(
                    pd.DataFrame({'DateTime': [last_index],
                                  self.y_name: [positions[1]],
                                  self.x_name: [positions[0]]}))

                logging.info(f'{self.positions}')
                self.y_data = None
                self.x_data = None
        else:
            logging.info(f'There are {len(events)} events.')

    def signal_data(self):
        return SignalData(self.__class__.__name__,
                          [("positions", SignalData.create_indexed_frame(
                              self.positions[[self.y_name, self.x_name]].fillna(method='ffill'))),
                           ("prices", SignalData.create_indexed_frame(self.prices))])
