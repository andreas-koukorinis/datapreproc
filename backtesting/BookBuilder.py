import TradeLogic

# The job of Book Builder is:
# Update the daily book[based on tuples(timestamp,closingprices)] on the 'ENDOFDAY' event corresponding to its product
class BookBuilder:
    def __init__(self,product,strategy,backtester,maxentries_dailybook,maxentries_intradaybook):
        self.product=product
        self.strategy = strategy
        self.backtester = backtester
        self.maxentries_dailybook = maxentries_dailybook
        self.maxentries_intradaybook = maxentries_intradaybook
        self.dailybook=[]         					#list of tuples (datetime,price)
        self.intradaybook=[]                                            #list of tuples (type,size,price) #type = 0 -> bid, type = 1 -> ask

    #Update the daily book with closing price and timestamp
    def dailyBookupdate(self,event,track):
        self.dailybook.append((event['dt'],event['price']))
        if(len(self.dailybook)>self.maxentries_dailybook):
            self.dailybook.pop(0)                                                                                  
        self.ondailyBookupdate(event,track)									#Call backtester,update indicators and call strategy for this event

    #TO BE COMPLETED
    #bidorask : bid=0,ask=1
    def intradayupdate(self,event):
        pass
 
    #Call backtester,update indicators and call strategy for this event    
    def ondailyBookupdate(self,event,track):
        self.backtester.updatePendingOrders(self.dailybook,event['dt'].date(),track)                            #Call Backtester to update the pending orders that have been filled
        for indicator in self.strategy.daily_indicators:						        #Update indicators,For each indicator written by user in TradeLogic.py
            indicatorfunc = getattr(self.strategy,indicator)                                                    #convert string name to function
            self.strategy.indicator_values[indicator+self.product] = indicatorfunc(self.dailybook,self.product) #call the indicator function and store the value
														#EG: if product = 'ES1' and indicatorfunc = Stddev,then 
														#update self.strategy.indicator_values['ES1Stddev']= Stddev(book,'ES1')
