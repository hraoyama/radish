import os
import sys
import pandas as pd
import uuid

sys.path.append(os.getenv("RADISH_PATH"))
sys.path.append(os.getenv("RADISH_DIR"))

from paprika.data.feed_filter import Filtration


class FeedSubscriber(object):
    def __init__(self):
        self.uuid = uuid.uuid4().hex
        self.filtrations = []
        self.call_count = 0
    
    def add_filtration(self, filtration: Filtration):
        self.filtrations.append(filtration)
    
    def clear_filtration(self):
        self.filtrations = []
    
    def handle_event(self, event: pd.DataFrame):
        self.call_count += 1
        print(event.shape)
        pass
    
    @property
    def uuid(self):
        return self.uuid
