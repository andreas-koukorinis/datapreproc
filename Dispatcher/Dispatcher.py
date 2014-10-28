import sys
from datetime import datetime,timedelta
import heapq
import ConfigParser
import MySQLdb
from Utils.DbQueries import db_connect,db_close,check_settlement_day
from Utils.Regular import get_dt_from_date,check_eod

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

    def __init__(self,products,_startdate,_enddate,config_file):
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))
        start_date = _startdate
        end_date = _enddate
        self.start_dt = get_dt_from_date(start_date)  # Convert date to datetime object with time hardcoded as 23:59:59:999999
        self.end_dt = get_dt_from_date(end_date)
        self.end_dt_sim = self.end_dt + timedelta (days=10)  # Since filled price depends on the next day,we need to run simluation till the trading day next to end_date
                                                             # Assumption is that after the end_date there will be a trading day within the next 10 days
        self.trading_days=0
        if config.has_option('Parameters', 'warmupdays'):
            warmupdays = config.getint('Parameters','warmupdays')
        else:
            warmupdays = 60  # Default value of warmupdays,in case not specified in config file

        self.sim_start_dt = self.start_dt + timedelta (days=-warmupdays)
        self.products = products
        self.heap = []	# Initialize the heap, heap will contain tuples of the form (timestamp,event)
        (self.dbconn, self.db_cursor) = db_connect()  # Initialize the database cursor
        self.event_listeners = dict([(product,[]) for product in self.products])  # For each product,maintain a list of listeners
        self.events_listeners = []  # These are the listeners which receive all the concurrent events at once.Here Strategy only
        self.end_of_day_listeners = []  # These are the listeners called on eand of each trading day.Here Performance Tracker

    @staticmethod
    def get_unique_instance(products,_startdate,_enddate,config_file):
        if(len(Dispatcher.instance)==0):
            new_instance = Dispatcher(products,_startdate,_enddate,config_file)
            Dispatcher.instance.append(new_instance)
        return Dispatcher.instance[0]

    def add_event_listener(self,listener,product):  # For Bookbuilders
        self.event_listeners[product].append(listener)

    def add_events_listener(self,listener):  # For strategy
        self.events_listeners.append(listener)

    def add_end_of_day_listener(self,listener):
        self.end_of_day_listeners.append(listener)

    # Main function which loops over the events and makes appropriate calls
    # ASSUMPTION:All ENDOFDAY events have same time
    def run(self):
        self.heap_initialize(self.products)  # Add all events for all the products to the heap
        current_dt = self.heap[0][0] # Get the lowest timestamp which has not been handled
        while(current_dt<=self.end_dt_sim ):   # Run simulation till one day after end date,so that end date orders can be filled
            last = self.end_dt.date()<current_dt.date()
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

            if( current_dt.date() >= self.start_dt.date() and current_dt.date() <= self.end_dt.date() ):  # If there are some events and warmupdays are over
                for listener in self.events_listeners:
                    listener.on_events_update(concurrent_events)  # Make 1 call to OnEventsUpdate of the strategy for all the concurrent events
            if( current_dt >= self.start_dt and check_eod(concurrent_events)):
                for listener in self.end_of_day_listeners:
                    listener.on_end_of_day(concurrent_events[0]['dt'].date())

                if(current_dt.date() <= self.end_dt.date()):  self.trading_days=self.trading_days+1

            if(len(self.heap)>0):
                current_dt = self.heap[0][0] # If the are still elements in the heap,update the timestamp to next timestamp
            else : break # If sufficient data is not available,break out of loop
                # TODO { } probably need to see if we need to fetch events here
                # Push the next daily event for this product
                #for _product in self.products :
                #    self.push_daily_event ( _product, current_dt + timedelta(days=1) )
            if(last): break # if we have surpassed end_date,stop the simulation
        db_close(self.dbconn)

    #Initialize the heap with 1 event for each source closest to startdate
    #TODO:Add intraday events also to the heap
    def heap_initialize(self,products):
        #Push DB EOD sources
        for _product in self.products:
            # Earlier we were pushing only the first event
            # self.pushdailyevent(_product, start_dt)
            # TODO { gchak } fetch all data for this product, not just first
            # and make events
            try:
                if(_product[0]=='f'):
                    _table_name = _product.rstrip('1234567890').lstrip('f')
                    _query = "SELECT Date," + _product.lstrip('f') + ",Spec FROM " + _table_name + " WHERE Date >= '" + str(self.sim_start_dt.date())+"' AND Date <= '" + str( ( self.end_dt_sim ).date() ) + "' ORDER BY Date";
                    self.db_cursor.execute(_query)
                    _data_list = self.db_cursor.fetchall() #should check if data exists or not
                    for _data_list_index in xrange ( 0, len(_data_list) ) :
                        _data_item = _data_list [ _data_list_index ]
                        _data_item_datetime = datetime.combine ( _data_item[0], datetime.max.time() )
                        _data_item_price = float(_data_item[1])
                        _data_item_symbol = _data_item[2]
                        _is_last_trading_day = False
                        if((_product[0] == 'f' ) and ( _data_list_index < ( len(_data_list) - 1 ) ) and ( _data_item_symbol != _data_list [ _data_list_index + 1 ][2] and _data_item_symbol != '#NA' and _data_list [ _data_list_index + 1 ][2] !='#NA' and len(_data_item_symbol)>0 and len(_data_list[_data_list_index+1][2])>0)): # To take care of '' and '#NA' present in Specific Symbol
                            _is_last_trading_day = True
                        _event = {'price': _data_item_price, 'product':_product, 'type':'ENDOFDAY', 'dt':_data_item_datetime, 'table':_table_name, 'is_last_trading_day':_is_last_trading_day}
                        heapq.heappush ( self.heap, ( _data_item_datetime, _event ) )
                else:
                    _table_name = _product
                    _query = "SELECT Date," + _product + " FROM " + _table_name + " WHERE Date >= '" + str(self.sim_start_dt.date())+"' AND Date <= '" + str( ( self.end_dt_sim ).date() ) + "' ORDER BY Date";
                    self.db_cursor.execute(_query)
                    _data_list = self.db_cursor.fetchall() #should check if data exists or not
                    for _data_list_index in xrange ( 0, len(_data_list) ) :
                        _data_item = _data_list [ _data_list_index ]
                        _data_item_datetime = datetime.combine ( _data_item[0], datetime.max.time() )
                        _data_item_price = float(_data_item[1])
                        _is_last_trading_day = False
                        _event = {'price': _data_item_price, 'product':_product, 'type':'ENDOFDAY', 'dt':_data_item_datetime, 'table':_table_name, 'is_last_trading_day':_is_last_trading_day}
                        heapq.heappush ( self.heap, ( _data_item_datetime, _event ) )

            except MySQLdb.Error, e:
                try:
                    print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                except IndexError:
                    print "MySQL Error: %s" % str(e)
                sys.exit("Error In DB.fetchall")
                # TODO {} log error in logfile and gracefully exit

        # TODO {} Push FLAT FILE sources


    # Given a product name and a timestamp(dt),fetch the next eligible ENDOFDAY event from the database
    # Settlement day for each future roduct is tracked,so that Daily log returns can be calculated properly and shifting on settlement day can be done
    def push_daily_event(self,product,dt):
        (date,price) = self.fetch_next_db ( product.rstrip('1234567890').lstrip('f'), product, dt)
        if(product[0]=='f'):
            is_last_trading_day = check_settlement_day ( self.db_cursor, product, date)
        else:
            is_last_trading_day = False
        dt = get_dt_from_date(date)
        event = {'price': price, 'product':product, 'type':'ENDOFDAY', 'dt':dt, 'table':product.rstrip('1234567890').lstrip('f'),'is_last_trading_day':is_last_trading_day}
        heapq.heappush ( self.heap, (dt,event) )

    # Given the tablename,product and datetime,fetch 1 record from the product's table with the least date greater than the given date
    # ASSUMPTION :Since db contains only dates,therefore convert given datetime to date and compare
    def fetch_next_db(self,table,product,dt):
        try:
            _query = "SELECT Date,"+product.lstrip('f')+" FROM "+table+" WHERE Date >= '"+str(dt.date())+"' ORDER BY Date LIMIT 1"
            self.db_cursor.execute(_query)
            data = self.db_cursor.fetchall() # Should check if data exists or not
            return (str(data[0][0]),float(data[0][1]))
        except:
            sys.exit("Error In DB.fetchnext")
