from paprika.data.feed_subscriber import FeedSubscriber

from paprika.core.base_signal import Signal
from paprika.data.fetcher import DataType
from paprika.data.feed_subscriber import FeedSubscriber

from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd
import re
from typing import List, Tuple
import logging

from paprika.signals.signal_data import SignalData
from scipy.stats import norm


class BollingerBands(FeedSubscriber, Signal):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.look_back = self.get_parameter("LOOKBACK")
        self.y_name = str(self.get_parameter("Y_NAME")).upper().strip()
        self.x_name = str(self.get_parameter("X_NAME")).upper().strip()
        self.y_name_m = re.compile("^" + self.y_name + ".*")
        self.x_name_m = re.compile("^" + self.x_name + ".*")
        
        self.positions = pd.DataFrame()
        self.prices = pd.DataFrame()
        self.probabilities = pd.DataFrame()
        self.hedge_ratios = pd.DataFrame()
        self.spread = pd.DataFrame()
        
        # if either trades come in at separate times or close prices come in a the same time
        # we should be able to handle either
        self.y_data = None
        self.x_data = None
        self.max_time_distance = self.get_parameter("MAX_TIME_DISTANCE", pd.Timedelta(np.timedelta64(10, 's')))
    
    def calc_position(self, z_score, hedge_ratio, last_index):
        longs_entry = -1
        shorts_entry = 1
        num_units_long = 1 if z_score < longs_entry else 0
        num_units_short = -1 if z_score > shorts_entry else 0
        num_units = num_units_long + num_units_short
        
        positions = np.array([num_units, num_units]) * np.array([-hedge_ratio, 1.0])
        
        self.positions = self.positions.append(
            pd.DataFrame({'DateTime': [last_index],
                          self.y_name: positions[0],
                          self.x_name: positions[1]}))
        pass
    
    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):
        super().handle_event(events)
        if len(events) == 1:
            event = events[0]
            if event[0] == DataType.TRADES:
                price_column = 'Price'
            elif event[0] == DataType.CANDLE:
                price_column = 'Close'
            else:
                raise TypeError(f'Can not find the price column for {str(event[0])}.')

            # price_column = 'Price' if event[0] == DataType.TRADES else 'Adj Close'
            data1 = event[1]
            y_data = data1[[self.y_name_m.match(x) is not None for x in data1['Symbol']]]
            if y_data is not None:
                if self.y_data is None:
                    self.y_data = y_data[:]
                else:
                    self.y_data = self.y_data.append(y_data[:])
            x_data = data1[[self.x_name_m.match(x) is not None for x in data1['Symbol']]]
            if x_data is not None:
                if self.x_data is None:
                    self.x_data = x_data[:]
                else:
                    self.x_data = self.x_data.append(x_data[:])
            execute = False
            last_index = max(self.y_data.index[-1], self.x_data.index[-1])
            if event[0] == DataType.TRADES:
                if self.y_data.shape[0] >= self.look_back & self.x_data.shape[0] == self.y_data.shape[0]:
                    if last_index - min(self.y_data.index[-1], self.x_data.index[-1]) > self.max_time_distance:
                        execute = True
            else:
                execute = (self.y_data.shape[0] >= self.look_back) and (self.x_data.shape[0] >= self.look_back)
            
            if execute:
                self.prices = self.prices.append(
                    pd.DataFrame({'DateTime': [last_index],
                                  self.y_name: self.y_data[price_column][-1],
                                  self.x_name: self.x_data[price_column][-1]}))
                
                regress_results = LinearRegression().fit(
                    self.y_data[price_column][-self.look_back:-1].values.reshape(-1, 1),
                    self.x_data[price_column][-self.look_back:-1].values.reshape(-1, 1))
                
                self.hedge_ratios = self.hedge_ratios.append(
                    pd.DataFrame({'DateTime': [last_index],
                                  'hedge_ratio': regress_results.coef_[0]}))
                
                calc_spread = (-regress_results.coef_[0] * self.y_data[price_column][-1] + self.x_data[price_column][-1])[0]
                
                self.spread = self.spread.append(
                    pd.DataFrame({'DateTime': [last_index],
                                  'spread': calc_spread}))
                
                if self.spread.shape[0] > self.look_back:
                    z_score = (calc_spread - np.mean(self.spread[-self.look_back:-1]['spread'].values)) / np.std(
                        self.spread[-self.look_back:-1]['spread'].values)
                    self.calc_position(z_score, regress_results.coef_[0], last_index)
        
        else:
            raise ValueError(
                f'There are {len(events)} events ({[str(ev[0]) for ev in events]}). Only one event type allowed.')
        
    def signal_data(self):
        # positions = signal.positions[[self.Y_NAME,self.X_NAME]]].fillna(method='ffill').values
        # prices = signal.prices
        return SignalData(self.__class__.__name__,
                          [("positions", SignalData.create_indexed_frame(
                              # self.positions[[self.y_name, self.x_name]].fillna(method='ffill'))),
                              self.positions.fillna(method='ffill'))),
                           ("prices", SignalData.create_indexed_frame(self.prices)),
                           ("probabilities", SignalData.create_indexed_frame(self.prices))])
