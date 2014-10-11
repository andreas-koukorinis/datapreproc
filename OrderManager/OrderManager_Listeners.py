from abc import ABCMeta,abstractmethod

class OrderManagerListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def OnSendOrder(self,order): pass
   
    @abstractmethod
    def OnCancelOrder(self,order): pass
