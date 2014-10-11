from abc import ABCMeta,abstractmethod

class DailyLogReturnsListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def OnDailyLogReturnsUpdate(self,product,DailyLogReturns): pass
