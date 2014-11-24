import sys
from datetime import datetime,timedelta
import heapq
import MySQLdb
from Utils.DbQueries import push_all_events
from Utils.Regular import get_dt_from_date,check_eod
from Utils import defaults

'''The job of the dispatcher is :
 1)To maintain a heap of all the event sources keyed by the timestamp(datetime object in python)
 Listeners : OnDailyEventUpdate -> BookBuilders ,        OnEventsUpdate -> Strategy,PerformanceTracker
 2)For each daily event call OnDailyEventUpdate on its listeners corresponding to the product for which the daily event has occured
   Here the book builder whose product's daily price event has  occured,will be called
 3)Once the books have been updated,call the OnEventsUpdate function on its listeners
   Here the strategy's OnEventsUpdate function will be called passing it all the cncurrent events
 4)Push the next events into the heap and repeat'''
class Dispatcher (object):

    instance=[]

    def __init__( self, products, _startdate, _enddate, _config ):
        self.start_dt = get_dt_from_date(_startdate)  # Convert date to datetime object with time hardcoded as 23:59:59:999999
        self.end_dt = get_dt_from_date(_enddate)
        self.trading_days=0
        if _config.has_option('Parameters', 'warmupdays'):
            warmupdays = _config.getint('Parameters','warmupdays')
        else:
            warmupdays = defaults.WARMUP_DAYS  # Default value of warmupdays,in case not specified in config file

        self.sim_start_dt = self.start_dt + timedelta (days=-warmupdays)
        self.products = products
        self.heap = []	# Initialize the heap, heap will contain tuples of the form (timestamp,event)
        self.event_listeners = dict([(product,[]) for product in self.products])  # For each product,maintain a list of listeners
        self.events_listeners = []  # These are the listeners which receive all the concurrent events at once.Here Strategy only
        self.end_of_day_listeners = []  # These are the listeners called on eand of each trading day.Here Performance Tracker

    @staticmethod
    def get_unique_instance( products, _startdate, _enddate, _config ):
        if len( Dispatcher.instance ) == 0 :
            new_instance = Dispatcher( products, _startdate, _enddate, _config )
            Dispatcher.instance.append( new_instance )
        return Dispatcher.instance[0]

    def add_event_listener( self, listener, product ):  # For Bookbuilders
        self.event_listeners[product].append( listener )

    def add_events_listener( self, listener ):  # For strategy
        self.events_listeners.append( listener )

    def add_end_of_day_listener( self, listener ): # For Performance Tracker
        self.end_of_day_listeners.append( listener )

    # Main function which loops over the events and makes appropriate calls
    # ASSUMPTION:All ENDOFDAY events have same time
    def run(self):
        self.heap_initialize(self.products)  # Add all events for all the products to the heap
        current_dt = self.heap[0][0] # Get the lowest timestamp which has not been handled
        while(current_dt<=self.end_dt ): 
            concurrent_events=[]
            while( ( len(self.heap)>0 ) and ( self.heap[0][0]==current_dt ) ) : # Add all the concurrent events for the current_dt to the list concurrent_events
                tup = heapq.heappop(self.heap)
                event = tup[1]
                concurrent_events.append(event)
            for event in concurrent_events:
                if(event['type']=='ENDOFDAY'): # This is an endofday event
                    for listener in self.event_listeners[event['product']]:
                        listener.on_daily_event_update(event)  # Call dailybookbuilder to update the book

                if(event['type']=='INTRADAY'):  # This is an intraday event
                    pass  # TODO:call intradaybookbuilder and push next

            if( current_dt.date() >= self.start_dt.date() ):  # If there are some events and warmupdays are over
                for listener in self.events_listeners:
                    listener.on_events_update(concurrent_events)  # Make 1 call to OnEventsUpdate of the strategy for all the concurrent events
                self.trading_days=self.trading_days+1

            if( current_dt >= self.start_dt and check_eod(concurrent_events)):
                for listener in self.end_of_day_listeners:
                    listener.on_end_of_day(concurrent_events[0]['dt'].date())

            if(len(self.heap)>0):
                current_dt = self.heap[0][0] # If the are still elements in the heap,update the timestamp to next timestamp
            else : break # If sufficient data is not available,break out of loop

    #Initialize the heap with all end of day events
    #TODO:Add intraday events also to the heap
    def heap_initialize(self,products):
        push_all_events(self.heap, products, self.sim_start_dt.date(), self.end_dt.date())     
