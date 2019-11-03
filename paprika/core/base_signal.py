from abc import ABC


class Signal(ABC):
    def __init__(self):
        super().__init__()

    def signal_data(self):
        pass
