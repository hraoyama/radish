import os
import sys
import pandas as pd
import uuid
from typing import List

sys.path.append(os.getenv("RADISH_PATH"))
sys.path.append(os.getenv("RADISH_DIR"))

from paprika.data.fetcher import DataChannel
from paprika.data.feed_filter import Filtration
from paprika.data.feed_subscription import DataType


class FeedSubscriber(object):
    def __init__(self):
        self._uuid = uuid.uuid4().hex
        self.filtrations = []
        self.data_types = []
        self.call_count = 0
    
    def add_data_type(self, data_type: DataType):
        if data_type not in self.data_types:
            self.data_types.append(data_type)
    
    def clear_data_types(self):
        self.data_types = []
    
    def add_filtration(self, filtration: Filtration):
        if filtration not in self.filtrations:
            self.filtrations.append(filtration)
    
    def clear_filtration(self):
        self.filtrations = []
    
    def handle_event(self, event: List[pd.DataFrame], data_type: List[DataType]):
        self.call_count += 1
        # print(event.shape)
        pass
    
    @property
    def uuid(self):
        return self._uuid
    
    

if __name__ == "__main__":
    a = FeedSubscriber()
