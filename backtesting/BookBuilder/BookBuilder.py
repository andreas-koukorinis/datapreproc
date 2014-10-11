import ConfigParser
from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import DailyEventListener

#BookBuilder listens to the dispatcher for daily update of its product
#Each product has a different bookbuilder
# The job of Book Builder is:
# Update the daily book[based on tuples(timestamp,closingprices)] on the 'ENDOFDAY' event corresponding to its product
# Call its Daily book listeners : Backtester,DailyLogReturn Indicator
class BookBuilder(DailyEventListener):

    instances={}

    def __init__(self,product,config_file):
        self.product=product
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))
        self.maxentries_dailybook = config.getint('Parameters', 'warmupdays')
        self.maxentries_intradaybook = config.getint('Parameters', 'maxentries_intradaybook')
        self.dailybook=[]         					#list of tuples (datetime,price)
        self.intradaybook=[]                                            #list of tuples (type,size,price) #type = 0 -> bid, type = 1 -> ask
        self.dailybook_listeners = []
        self.intradaybook_listeners = []
        products = config.get('Products', 'symbols').strip().split(",")
        dispatcher = Dispatcher.GetUniqueInstance(products,config_file)
        dispatcher.AddEventListener(self) 

    @staticmethod
    def GetUniqueInstance(product,config_file):
        if(product not in BookBuilder.instances.keys()):
            new_instance = BookBuilder(product,config_file)    
            BookBuilder.instances[product]=new_instance
        return BookBuilder.instances[product]

    def AddDailyBookListener(self,listener):
        self.dailybook_listeners.append(listener)

    def AddIntradayBookListener(self,listener):
        self.intradaybook_listeners.append(listener)

    #Update the daily book with closing price and timestamp
    def OnDailyEventUpdate(self,event):
        # Should we store is_settlement_day also?
        self.dailybook.append((event['dt'],event['price']))                                                #Add entry to the book.If max entries are reached pop first entry
        if(len(self.dailybook)>self.maxentries_dailybook):                               
            self.dailybook.pop(0)                                                                                  
        for listener in self.dailybook_listeners:
            listener.OnDailyBookUpdate(self.product,self.dailybook,event['is_settlement_day'])             #Pass the full dailybook to its listeners

    #TO BE COMPLETED
    #bidorask : bid=0,ask=1
    def OnIntradayEventUpdate(self,event):
        pass

