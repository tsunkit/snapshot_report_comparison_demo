from abc import ABC, abstractmethod

class DataSourceBase(ABC):
    @abstractmethod
    def get_engine(self):
        pass

    @abstractmethod
    def connect_data(self):
        pass

"""
class DataSourceBase:
    def get_engine(self):
        raise NotImplementedError("Subclasses must implement get_engine()")
"""