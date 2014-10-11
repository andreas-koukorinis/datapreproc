import sys
import datetime
import heapq
import ConfigParser
from Utils.DbQueries import db_connect,db_close,check_settlement_day
from Utils.Regular import getdtfromdate

#The job of the dispatcher is
#1)To maintain a heap of all the event sources keyed by the timestamp(datetime object in python)
#Listeners : OnDailyEventUpdate -> BookBuilders ,        OnEventsUpdate -> Strategy,PerformanceTracker
#2)For each daily event call OnDailyEventUpdate on its listeners corresponding to the product for which the daily event has occured
##Here the book builder whose product's daily price event has  occured,will be called
#3)Once the books have been updated,call the OnEventsUpdate function on its listeners
##Here the strategy's OnEventsUpdate function will be called passing it all the cncurrent events
#4)Push the next events into the heap and repeat
class Dispatcher (object):

    instance=[]

    def __init__(self,products,config_file):
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))
        start_date = config.get('Dates','start_date')
        end_date = config.get('Dates','end_date')
        self.start_dt = getdtfromdate(start_date)                                               #Convert date to datetime object with time hardcoded as 23:59:59:999999
        self.end_dt = getdtfromdate(end_date)
        self.trading_days=0
        warmupdays = config.getint('Parameters','warmupdays')
        self.strategy_start_dt = self.start_dt+datetime.timedelta(days=warmupdays)
        self.products = products
        self.heap = []										#Initialize the heap,heap will contain tuples of the form (timestamp,event)
        (self.dbconn,self.db_cursor) = db_connect()                                             #Initialize the database cursor
        self.eventlisteners = []                                                                #These are the listeners which receive 1 daily event for their product.Here bookbuilders
        self.eventslisteners = []                                                               #These are the listeners which receive all the concurrent events at once.Here Strategy and Performance Tracker

    @staticmethod
    def GetUniqueInstance(products,config_file):
        if(len(Dispatcher.instance)==0):
            new_instance = Dispatcher(products,config_file)
            Dispatcher.instance.append(new_instance)
        return Dispatcher.instance[0]

    def AddEventListener(self,listener):                                                        #For Bookbuilders
        self.eventlisteners.append(listener)

    def AddEventsListener(self,listener):                                                       #For strategy and Performance Tracker
        self.eventslisteners.append(listener)

    #Main function which loops over the events and makes appropriate calls
    #ASSUMPTION:All ENDOFDAY events have same time
    def run(self):
        self.heap_initialize(self.products)							#Add 1 earliest event after the startdate from each source
        current_dt = heapq.nsmallest(1,self.heap)[0][0]                                         #Get the lowest timestamp which has not been handled
        while(current_dt<=self.end_dt):                                                         #While timestamp does not surpass the end date
            concurrent_events=[]
            while(len(self.heap)>0 and heapq.nsmallest(1,self.heap)[0][0]==current_dt):         #Add all the concurrent events for the current_dt to the list concurrent_events
                tup = heapq.heappop(self.heap)
                event = tup[1]
                concurrent_events.append(event)
            for event in concurrent_events:
                if(event['type']=='ENDOFDAY'):                                                  #This is an endofday event
                    for listener in self.eventlisteners:
                        if(listener.product==event['product']):
                            listener.OnDailyEventUpdate(event)                                  #call dailybookbuilder to update the book

                    self.pushdailyevent(event['product'],current_dt+datetime.timedelta(days=1)) #Push the next daily event for this product

                if(event['type']=='INTRADAY'):                                                  #This is an intraday event
                    pass                                                                        #TO BE COMPLETED:call intradaybookbuilder and push next
            #assert self.strategy.portfolio.get_portfolio()['cash']>=0                          #After every set of concurrent events,portfolio cash should be non negative

            if(len(concurrent_events)>0 and current_dt >= self.strategy_start_dt):              #if there are some events and warmupdays are over
                for listener in self.eventslisteners:
                    listener.OnEventsUpdate(concurrent_events)                                  #Make 1 call to OnEventsUpdate of the strategy and Performance Tracker for all the concurrent events
                self.trading_days=self.trading_days+1

            if(len(self.heap)>0):
                current_dt = heapq.nsmallest(1,self.heap)[0][0]                                 #If the are still elements in the heap,update the timestamp to next timestamp

        db_close(self.dbconn)									#Close database connection

    #Initialize the heap with 1 event for each source closest to startdate
    #TO BE COMPLETED:Add intraday events also to the heap
    def heap_initialize(self,products):
        #Push DB EOD sources
        for product in products:
            self.pushdailyevent(product,self.start_dt)
        #Push FLAT FILE sources
        #TO BE DONE

    #given a product name and a timestamp(dt),fetch the next eligible ENDOFDAY event from the database
    #Settlement day for each future roduct is tracked,so that Daily log returns can be calculated properly and shifting on settlement day can be done
    def pushdailyevent(self,product,dt):
        (date,price) = self.fetchnextdb(product.rstrip('1234567890').lstrip('f'),product,dt)
        if(product[0]=='f'):
            is_settlement_day = check_settlement_day(self.db_cursor,product,date)
        else:
            is_settlement_day = False
        dt = getdtfromdate(date)
        event = {'price': price, 'product':product, 'type':'ENDOFDAY', 'dt':dt, 'table':product.rstrip('1234567890').lstrip('f'),'is_settlement_day':is_settlement_day}
        heapq.heappush(self.heap,(dt,event))

    #Given the tablename,product and datetime,fetch 1 record from the product's table with the least date greater than the given date
    #ASSUMPTION :Since db contains only dates,therefore convert given datetime to date and compare
    def fetchnextdb(self,table,product,dt):
        try:
            query = "SELECT Date,"+product.lstrip('f')+" FROM "+table+" WHERE Date >= '"+str(dt.date())+"' ORDER BY Date LIMIT 1"
            self.db_cursor.execute(query)
            data = self.db_cursor.fetchall()                                                      #should check if data exists or not
            return (str(data[0][0]),float(data[0][1]))
        except:
            sys.exit("Error In DB.fetchnext")
