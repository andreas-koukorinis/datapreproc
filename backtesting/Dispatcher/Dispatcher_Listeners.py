from abc import ABCMeta,abstractmethod

class DailyEventListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def OnDailyEventUpdate(self,event): pass

class EventsListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def OnEventsUpdate(self,concurrent_events): pass

