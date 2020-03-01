from paprika.core.base_signal import Signal
from paprika.data.fetcher import DataType
from paprika.data.feed_subscriber import FeedSubscriber
from paprika.signals.signal_data import SignalData
from paprika.data.data_channel import DataChannel
from paprika.alpha.base import Alpha
from paprika.data.data_processor import DataProcessor
from paprika.data.feed_filter import TimeFreqFilter, TimePeriod

import datetime as dt
import numpy as np
import pandas as pd
import re
from typing import List, Tuple
import logging


class AlphaUnit(FeedSubscriber, Signal):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.alpha_unit = self.get_parameter("alpha")
        self.positions = pd.DataFrame()
        self.prices = pd.DataFrame()
        self.probabilities = pd.DataFrame()

    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):

        super().handle_event(events)

        if len(events) == 1:
            event = events[0]
            data = event[1]
            date = pd.to_datetime(data.index.unique().values[-1])
            data = data.reset_index().set_index([DataChannel.SYMBOL_INDEX, DataChannel.DATA_INDEX])
            # data.reset_index().set_index([DataChannel.DATA_INDEX, DataChannel.SYMBOL_INDEX])
            # symbols = data['Symbol'].unique().tolist()
            # symbols = DataChannel.symbols_remove_data_type(symbols)
            # dfs = DataChannel.fetch(symbols, end=date)
            dp = DataProcessor(data)
            # dp.ohlcv(time_filter, inplace=True)
            alpha = Alpha(dp)
            alpha.add_alpha(self.alpha_unit)
            # # for alpha_unit in alpha.list_alpha():
            res = alpha[self.alpha_unit.__name__]
            if res.shape[0] > 0:
                res = res.loc[date]
                position = res.apply(lambda x: 1 if x > 0 else 0)
                self.positions = self.positions.append(position)
                self.prices = self.prices.append(dp.close.loc[date])
                self.probabilities = self.probabilities.append(position.apply(lambda x: 1))

    def signal_data(self):
        self.positions.index.name = DataChannel.DATA_INDEX
        self.probabilities.index.name = DataChannel.DATA_INDEX
        self.prices.index.name = DataChannel.DATA_INDEX
        return SignalData(self.__class__.__name__,
                          [("positions", SignalData.create_indexed_frame(
                              # self.positions[[self.y_name, self.x_name]].fillna(method='ffill'))),
                              self.positions.fillna(method='ffill'))),
                           ("prices", SignalData.create_indexed_frame(self.prices)),
                           ("probabilities", SignalData.create_indexed_frame(self.prices))])
