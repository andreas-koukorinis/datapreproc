import ConfigParser
from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import DailyEventListener
from Utils.Regular import get_dt_from_date

'''BookBuilder listens to the dispatcher for daily update of its product
 Each product has a different bookbuilder
 The job of Book Builder is:
 Update the daily book[based on tuples(timestamp,closingprices)] on the 'ENDOFDAY' event corresponding to its product
 Call its Daily book listeners : Indicators'''
class BookBuilder(DailyEventListener):

    instances={}

    def __init__(self,product,config_file):
        self.product=product
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))
        self.maxentries_dailybook = config.getint('Parameters', 'maxentries_dailybook')
        self.maxentries_intradaybook = config.getint('Parameters', 'maxentries_intradaybook')
        self.dailybook=[]  # List of tuples (datetime,price,is_last_trading_day)
        self.intradaybook=[]  # List of tuples (type,size,price) # Type = 0 -> bid, type = 1 -> ask
        self.dailybook_listeners = []
        self.intradaybook_listeners = []
        products = config.get('Products', 'symbols').strip().split(",")
        dispatcher = Dispatcher.get_unique_instance(products,config_file)
        dispatcher.add_event_listener(self) 

    @staticmethod
    def get_unique_instance(product,config_file):
        if(product not in BookBuilder.instances.keys()):
            new_instance = BookBuilder(product,config_file)    
            BookBuilder.instances[product]=new_instance
        return BookBuilder.instances[product]

    def add_dailybook_listener(self,listener):
        self.dailybook_listeners.append(listener)

    def add_intradaybook_listener(self,listener):
        self.intradaybook_listeners.append(listener)

    # Update the daily book with closing price and timestamp
    def on_daily_event_update(self,event):
        self.dailybook.append((event['dt'],event['price'],event['is_last_trading_day']))  # Add entry to the book.If max entries are reached pop first entry
        if(len(self.dailybook)>self.maxentries_dailybook):                               
            self.dailybook.pop(0)     
        for listener in self.dailybook_listeners:
            listener.on_dailybook_update(self.product,self.dailybook)  # Pass the full dailybook to its listeners

    # TODO
    # bidorask : bid=0,ask=1
    def on_intraday_event_update(self,event):
        pass

