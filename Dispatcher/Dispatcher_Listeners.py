from abc import ABCMeta,abstractmethod

class DailyEventListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def on_daily_event_update(self,event): pass

class EventsListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def on_events_update(self,concurrent_events): pass

class EndOfDayListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def on_end_of_day(self,date): pass

