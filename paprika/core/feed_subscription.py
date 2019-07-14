

from datetime import datetime
import re

class FeedSubscription:
    
    def __init__(self, start: datetime, end: datetime):
        super().__init__()
        self.start_datetime = start
        self.end_datetime = end

    def add_feed(self, matchPatterns):
        pass
    
    