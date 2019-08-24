import os
import sys
import pandas as pd
import uuid
from typing import List, Tuple
from abc import ABC

sys.path.append(os.getenv("RADISH_PATH"))
sys.path.append(os.getenv("RADISH_DIR"))

from paprika.data.feed_filter import Filtration
from paprika.data.feed import DataType


class FeedSubscriber(ABC):
    def __init__(self, **kwargs):
        self._uuid = uuid.uuid4().hex
        self.filtrations = []
        self.call_count = 0
        self._subscribed_feed = None
        self._parameter_dict = kwargs
    
    def add_filtration(self, filtration: Filtration):
        if filtration not in self.filtrations:
            self.filtrations.append(filtration)
    
    def clear_filtration(self):
        self.filtrations = []
    
    def handle_event(self, event: List[Tuple[DataType, pd.DataFrame]]):
        self.call_count += 1
        # print(event.shape)
        pass
    
    def run(self):
        if not self.subscribed_feed:
            raise ValueError(f'Signal is not subscribed to any feed')
        self.subscribed_feed.run(self)
    
    @property
    def subscribed_feed(self):
        return self._subscribed_feed
    
    @subscribed_feed.setter
    def subscribed_feed(self, value):
        from paprika.data.feed import Feed
        assert isinstance(value, Feed)
        self._subscribed_feed = value
    
    def clear_feed(self, clear_call_count=True):
        self._subscribed_feed = None
        if clear_call_count:
            self.call_count = 0
    
    @property
    def uuid(self):
        return self._uuid
    
    @property
    def parameters(self):
        return self._parameter_dict.keys()
    
    def get_parameter(self, key, default_value=None):
        return self._parameter_dict.get(key, default_value)
    
    def set_parameter(self, key, value):
        self._parameter_dict[key] = value
