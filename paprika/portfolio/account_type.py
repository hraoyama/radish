from enum import Enum


class AccountType(Enum):
    SPOT = 0,
    MARGIN = 1,
    FUTURE = 2
