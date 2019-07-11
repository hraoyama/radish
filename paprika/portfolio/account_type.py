from enum import Enum


class AccountType(Enum):
    EXCHANGE = 0,
    MARGIN = 1,
    FUTURE = 2,
    FUNDING = 3
