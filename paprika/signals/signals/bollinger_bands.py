from paprika.core.base_signal import Signal
from paprika.data.fetcher import DataType
from paprika.data.feed_subscriber import FeedSubscriber
from paprika.signals.signal_data import SignalData
from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd
import re
from typing import List, Tuple
import logging


class BollingerBands(FeedSubscriber, Signal):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.lookback = self.get_parameter("LOOKBACK")
        self.y_name = str(self.get_parameter("Y_NAME")).upper().strip()
        self.x_name = str(self.get_parameter("X_NAME")).upper().strip()
        self.y_name_m = re.compile("^" + self.y_name + ".*")
        self.x_name_m = re.compile("^" + self.x_name + ".*")
        
        self.positions = pd.DataFrame()
        self.prices = pd.DataFrame()
        self.spreads = []
        self.longs_and_shorts = []
        self.zscores = []
        
        # if either trades come in at separate times or close prices come in a the same time
        # we should be able to handle either
        self.y_data = None
        self.x_data = None
        self.max_time_distance = self.get_parameter("MAX_TIME_DISTANCE", pd.Timedelta(np.timedelta64(10, 's')))

    @staticmethod
    def calc_long_short(z_score):

        longs_entry = -1
        longs_exit = 0
        shorts_entry = 1
        shorts_exit = 0

        num_units_long = np.nan
        if z_score < longs_entry:
            num_units_long = 1
        if z_score >= longs_exit:
            num_units_long = 0

        num_units_short = np.nan
        if z_score > shorts_entry:
            num_units_short = -1
        if z_score <= shorts_exit:
            num_units_short = 0

        return num_units_long, num_units_short

    def calc_position(self, z_score, beta, last_index):

        num_units_long, num_units_short = self.calc_long_short(z_score)

        if not self.longs_and_shorts:
            num_units_long_prev, num_units_short_prev = (0, 0)
        else:
            num_units_long_prev, num_units_short_prev = self.longs_and_shorts[-1]

        if num_units_long is np.nan:
            num_units_long = num_units_long_prev

        if num_units_short is np.nan:
            num_units_short = num_units_short_prev

        self.longs_and_shorts.append((num_units_long, num_units_short))
        num_units = num_units_long + num_units_short

        self.positions = self.positions.append(
            pd.DataFrame({'DateTime': [last_index],
                          self.y_name: [num_units * self.prices[self.y_name][-1:].values[0]],
                          self.x_name: [-num_units * beta * self.prices[self.x_name][-1:].values[0]]}))

    def calc_beta(self):
        regress_results = LinearRegression().fit(self.prices.iloc[-self.lookback:][self.x_name].values.reshape(-1, 1),
                                                 self.prices.iloc[-self.lookback:][self.y_name].values)
        return regress_results.coef_[0]

    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):

        super().handle_event(events)
        if len(events) == 1:
            event = events[0]
            if event[0] == DataType.TRADES:
                price_column = 'Price'
            elif event[0] == DataType.CANDLE:
                price_column = 'Close'
            elif event[0] == DataType.OHLCVAC_PRICE:
                price_column = 'Adj Close'
            else:
                raise TypeError(f'Can not find the price column for {str(event[0])}.')

            data1 = event[1]
            y_data = data1[[self.y_name_m.match(x) is not None for x in data1['Symbol']]]

            if y_data is not None:
                if self.y_data is None:
                    self.y_data = y_data
                # else:
                #    self.y_data = self.y_data.append(y_data[:])
            x_data = data1[[self.x_name_m.match(x) is not None for x in data1['Symbol']]]
            if x_data is not None:
                if self.x_data is None:
                    self.x_data = x_data[:]
                # else:
                #    self.x_data = self.x_data.append(x_data[:])
            execute = False
            last_index = max(self.y_data.index[-1], self.x_data.index[-1])
            if event[0] == DataType.TRADES:
                if self.y_data.shape[0] >= self.lookback & self.x_data.shape[0] == self.y_data.shape[0]:
                    if last_index - min(self.y_data.index[-1], self.x_data.index[-1]) > self.max_time_distance:
                        execute = True
            else:
                # execute = (self.y_data.shape[0] >= self.lookback) and (self.x_data.shape[0] >= self.lookback)
                # self.spreads = self.spreads.append(pd.DataFrame({'DateTime': [last_index], 'spread': np.nan}))
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
                    calc_spread = self.y_data[price_column][-1] - beta * self.x_data[price_column][-1]

                    self.spreads.append(calc_spread)
                    z_score = (calc_spread - np.mean(self.spreads[-self.lookback:])) /\
                              np.std(self.spreads[-self.lookback:], ddof=1)
                    self.zscores.append(z_score)
                    self.calc_position(z_score, beta, last_index)

                    logging.info(f'{self.positions}')
                    self.y_data = None
                    self.x_data = None
        
        else:
            raise ValueError(
                f'There are {len(events)} events ({[str(ev[0]) for ev in events]}). Only one event type allowed.')
        
    def signal_data(self):

        return SignalData(self.__class__.__name__,
                          [("positions", SignalData.create_indexed_frame(
                              # self.positions[[self.y_name, self.x_name]].fillna(method='ffill'))),
                              self.positions.fillna(method='ffill'))),
                           ("prices", SignalData.create_indexed_frame(self.prices)),
                           ("probabilities", SignalData.create_indexed_frame(self.prices))])
