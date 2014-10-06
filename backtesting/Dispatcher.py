import sys
import datetime
import heapq
from Utils import getdtfromdate,db_connect,db_close,check_settlement_day
from BookBuilder import BookBuilder

#The job of the dispatcher is 
#1)To maintain a heap of all the event sources keyed by the timestamp(datetime object in python)
#2)Group all the events with the smallest timestamp in the heap and call Book Builders on each of these events
#3)Once the books have been updated,call the strategy passing these group of events
#4)Push the next events into the heap and repeat
class Dispatcher:
    
    def __init__(self,start_date,end_date,products,bb_objects,strategy,warmupdays):
        self.start_dt = getdtfromdate(start_date)
        self.end_dt = getdtfromdate(end_date)
        self.products = products
        self.heap = []										#Initialize the heap,heap will contain tuples of the form (timestamp,event)
        (self.dbconn,self.db_cursor) = db_connect()                                             #Initialize the database cursor
        self.bb_objects = bb_objects                                                            
        self.strategy = strategy
        self.days = 0										#To check warmup period
        self.warmupdays = warmupdays

    #Main function which loops over the events and makes appropriate calls    
    #ASSUMPTION:All ENDOFDAY events have same time
    def run(self):
        self.heap_initialize(self.products)							#Add 1 earliest event after the startdate from each source
        current_dt = heapq.nsmallest(1,self.heap)[0][0]                                         #Get the lowest timestamp which has not been handled               
        while(current_dt<=self.end_dt):                                                         #While timestamp does not surpass the end date
            concurrent_events=[]       
            done=0                                                      
            while(len(self.heap)>0 and heapq.nsmallest(1,self.heap)[0][0]==current_dt):         #Add all the concurrent events for the current_dt to the list concurrent_events    
                tup = heapq.heappop(self.heap)
                event = tup[1]
                concurrent_events.append(event)							
            for event in concurrent_events: 
                if(event['type']=='ENDOFDAY'):                                                  #This is an endofday event
                    if(done==0):                                                                #Update the current day's number,f it has not been updated
                        done=1
                        self.days = self.days+1
                    if(self.days<=self.warmupdays):                                             #dont track performance if still in warmup period
		        track=0
                    else: track=1                        
                    self.bb_objects[event['product']].dailyBookupdate(event,track)  		#call dailybookbuilder to update the book
                    self.pushdailyevent(event['product'],current_dt+datetime.timedelta(days=1)) #Push the next daily event for this product
                if(event['type']=='INTRADAY'):                                                  #This is an intraday event
                    pass                                                                        #TO BE COMPLETED:call intradaybookbuilder and push next
            if(len(self.heap)>0):
                current_dt = heapq.nsmallest(1,self.heap)[0][0]                                 #If the are still elements in the heap,update the timestamp to next timestamp
            if(len(concurrent_events)>0 and track==1):                                                    
                self.strategy.OnEventListener(concurrent_events)                                #Make 1 call to the  oneventlistener of the strategy for all the concurrent events
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
    def pushdailyevent(self,product,dt):
        (date,price) = self.fetchnextdb(product.rstrip('1234567890').lstrip('f'),product,dt)
        if(product[0]=='f' and product[-1]=='1'):
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
