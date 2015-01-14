# cython: profile=True
from abc import ABCMeta,abstractmethod

class IndicatorListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def on_indicator_update(self,identifier,indicator_value): pass
