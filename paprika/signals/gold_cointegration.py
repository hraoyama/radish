import numpy as np
import pandas as pd
from typing import List, Tuple
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from collections import deque
from pprint import pprint as pp

from paprika.data.fetcher import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.feed import Feed
from paprika.data.feed_subscriber import FeedSubscriber
from paprika.data.feed_filter import TimeFreqFilter, Filtration, TimePeriod


class GoldSpread(FeedSubscriber):
    def __init__(self, **kwargs):
        super(GoldSpread, self).__init__(**kwargs)
        self.beta = self.get_parameter("BETA")
        self.mean = self.get_parameter("MEAN")
        self.sigma = self.get_parameter("STD")
        self.entry = self.get_parameter("ENTRY")
        self.exit = self.get_parameter("EXIT")
        self.positions = pd.DataFrame()

    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):
        super(GoldSpread, self).handle_event(events)
        data = events[0][1]
        z_score = (data['GLD'][0] - self.beta*data['GDX'][0] - self.mean) / self.sigma
        timestamp = data.index[0]
        if z_score < -self.entry:
            self.positions = self.positions.append(pd.DataFrame({'DateTime': [timestamp], 'GLD': [1.0], 'GDX': [-1.0]}))
        elif z_score > self.entry:
            self.positions = self.positions.append(pd.DataFrame({'DateTime': [timestamp], 'GLD': [-1.0], 'GDX': [1.0]}))
        else:
            self.positions = self.positions.append(pd.DataFrame({'DateTime': [timestamp], 'GLD': [0.0], 'GDX': [0.0]}))



