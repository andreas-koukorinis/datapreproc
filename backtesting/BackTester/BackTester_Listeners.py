from abc import ABCMeta,abstractmethod

class BackTesterListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def OnOrderUpdate(self,filled_orders,date): pass
   
    @abstractmethod
    def AfterSettlementDay(self,product): pass
