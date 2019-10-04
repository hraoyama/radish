from aenum import Enum
from aenum import extend_enum


class DataType(Enum):
    ORDERBOOK = 0
    TRADES = 1
    OHLCVAC_PRICE = 2
    
    def __str__(self):
        if self.value == 0:
            return 'OrderBook'
        elif self.value == 1:
            return 'Trade'
        else:
            return self.name
    
    @classmethod
    def extend(cls, name):
        value = len(list(cls))
        extend_enum(cls, name, value)
