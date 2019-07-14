from abc import ABC
from paprika.core.feed_subscription import FeedSubscription

class BaseSignal(ABC):
    def __init__(self):
        self.mongodb_host = 'localhost'
        self.redis_host = 'localhost'

class Signal(BaseSignal):
    def __init__(self, feedsubs: FeedSubscription):
        super().__init__()
        self.feed = feedsubs
