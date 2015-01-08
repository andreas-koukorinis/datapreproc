import sys
from datetime import datetime,timedelta
import heapq
import MySQLdb
from utils.dbqueries import push_all_end_of_day_events, push_all_tax_payment_events
from utils.regular import get_dt_from_date,check_eod
from utils import defaults

class Dispatcher (object):
    """Maintains a heap of all the future events, fetches them in chronological order
       and ensures the required workflow of the simulation

       Description: Initially the dispatcher fetches all end of day events from the database and pushes them to a heap keyed by the timestamp
                    Then for each set of concurrent events in chronological order,calls are made to(in order):
                    event_listeners: book builders updated
                    events_listeners: control given to strategy
                    end_of_day_listeners: performance updated

       Listeners: There are currently three types of dispatcher listeners to the dispatcher
                  1) event_listeners: The listeners which are interested in the events corresponding to a particular product.
                                      Names: Bookbuilders for each product
                  2) events_listeners: The listeners which are interested in all sets of concurrent events
                                       Names: TradeAlgorithm and SignalAlgorithm
                  3) end_of_day_listeners: The listeners which are interested in one notification per day
                                           Names: PerformanceTracker

       Listening to: None
    """

    instance=[]

    def __init__( self, products, _startdate, _enddate, _config ):
        """Initializes the required variables.
           
           Args:
               products(list): The exhaustive list of products we end up trading.Eg: ['fES_1','fES_2','AQRIX']
               _startdate(date object): The start date of the simulation
               _enddate(date object): The end date of the simulation
               _config(ConfigParser handle): The handle to the config file of the strategy
            
           Note:
               1) Warmupdays are used to start the computation of indicators earlier starting the actual simulation.
                  The actual siumation is always from the start date to the end date
        """
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
        self.tax_payment_day_listeners = [] # These are the listeners called on the tax payment day(typically end of year).Here Performance Tracker

    @staticmethod
    def get_unique_instance( products, _startdate, _enddate, _config ):
        """This static function is used by other classes to add themselves as a listener to the dispatcher"""
        if len( Dispatcher.instance ) == 0 :
            new_instance = Dispatcher( products, _startdate, _enddate, _config )
            Dispatcher.instance.append( new_instance )
        return Dispatcher.instance[0]

    def add_event_listener( self, listener, product ):  # For Bookbuilders
        """Used by classes to register as event_listener of the dispatcher

           Args:
               listener(object): The object of the class which wants to listen
               product(string): The product whose events are to be listened to

           Returns: Nothing       
        """
        self.event_listeners[product].append( listener )

    def add_events_listener( self, listener ):  # For strategy,signals
        """Used by classes to register as events_listener of the dispatcher

           Args:
               listener(object): The object of the class which wants to listen

           Returns: Nothing       
        """
        self.events_listeners.append( listener )

    def add_end_of_day_listener( self, listener ): # For Performance Tracker
        """Used by classes to register as end_of_day_listener of the dispatcher

           Args:
               listener(object): The object of the class which wants to listen

           Returns: Nothing       
        """
        self.end_of_day_listeners.append( listener )

    def add_tax_payment_day_listener( self, listener ): # For Performance Tracker
        """Used by classes to register as end_of_day_listener of the dispatcher

           Args:
               listener(object): The object of the class which wants to listen

           Returns: Nothing       
        """
        self.tax_payment_day_listeners.append( listener )

    # ASSUMPTION:All ENDOFDAY events have same time
    def run(self):
        """All the functionality of dispatcher resides in this function

           Args: None

           Returns: Nothing
        """
        self.heap_initialize(self.products)
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
                        listener.on_daily_event_update(event)

                if(event['type']=='INTRADAY'):  # This is an intraday event
                    pass  # TODO:call intradaybookbuilder and push next

                if event['type'] == 'TAXPAYMENTDAY':
                    for listener in self.tax_payment_day_listeners:
                        listener.on_tax_payment_day()

            if( current_dt.date() >= self.start_dt.date() ):  # If warmupdays are over
                for listener in self.events_listeners:
                    listener.on_events_update(concurrent_events)
                self.trading_days=self.trading_days+1

            if( current_dt >= self.start_dt and check_eod(concurrent_events)):
                for listener in self.end_of_day_listeners:
                    listener.on_end_of_day(concurrent_events[0]['dt'].date())

            if(len(self.heap)>0):
                current_dt = self.heap[0][0] # If the are still elements in the heap,update the timestamp to next timestamp
            else : break # If sufficient data is not available,break out of loop

    #TODO:Add intraday events also to the heap
    def heap_initialize(self,products):
        """Initialize the heap with all end of day events

           Args:
               products(list): The products whose events are to be pushed to the heap

           Returns: Nothing      
        """
        push_all_end_of_day_events(self.heap, products, self.sim_start_dt.date(), self.end_dt.date())
        push_all_tax_payment_events(self.heap, self.start_dt.date(), self.end_dt.date())
