from abc import ABC, abstractmethod


class Algorithm(ABC):
    @abstractmethod
    def initialise(self):
        pass
