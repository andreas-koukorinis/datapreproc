from numpy import *
import datetime
from BookBuilder.BookBuilder_Listeners import DailyBookListener
from BookBuilder.BookBuilder import BookBuilder

# Track the daily log returns for the product
class DailyLogReturns(DailyBookListener):

    instances = {}

    def __init__(self,product,config_file):
        self.listeners=[]
        self.values = []
        self.prices=[0,0]  # Remember last two prices for the product #prices[0] is latest
        self.dt=[datetime.datetime.fromtimestamp(1),datetime.datetime.fromtimestamp(1)] # Last update dt for futures pair      
        self.product=product
        bookbuilder = BookBuilder.get_unique_instance(product,config_file)
        bookbuilder.add_dailybook_listener(self)
        if(product[0]=='f' and product[-1]=='1'):
            self.price2=0 # Remember the last price for the 2nd future contract if this is the first futures contract
            product2 = product.rstrip('1')+'2'
            bookbuilder = BookBuilder.get_unique_instance(product2,config_file)
            bookbuilder.add_dailybook_listener(self)

    def add_listener(self,listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(product,config_file):
        if(product not in DailyLogReturns.instances.keys()):
            new_instance = DailyLogReturns(product,config_file)
            DailyLogReturns.instances[product] = new_instance
        return DailyLogReturns.instances[product]

    # Update the daily log returns on each ENDOFDAY event
    def on_dailybook_update(self,product,dailybook):
        if((product[0]=='f' and product[-1]=='1') or product[0]!='f'): # For first futures contract and non-futures,update 0 index of dt 
            self.dt[0]=dailybook[-1][0]
            self.prices[1]=self.prices[0]
            self.prices[0]=dailybook[-1][1]
        else:
            self.dt[1]=dailybook[-1][0] # For non-first(second) future contracts,update 1st index of dt
            self.price2=dailybook[-1][1] 

        updated=False
        if(len(dailybook)>1):
            _yesterday_settlement = dailybook[-2][2]
        else:
            _yesterday_settlement = False

        if(product[0]=='f' and _yesterday_settlement and self.dt[0]==self.dt[1]):  
            # If its the futures contract,yesterday was the settlement day and price for both 1st and 2nd contract has been updated
            product1 = product.rstrip('1')+'1'  # Example : product1 = fES1
            product2 = product.rstrip('1')+'2'  # Example : product2 = fES2
            p1 = self.prices[0]
            p2 = self.price2
            if(p2!=0):
                logret = log(p1/p2)
            else:
                logret = 0  # If last two prices not available for a product,let logreturn = 0
            updated=True
        elif(product[0]!='f' or (product[0]=='f' and product[-1]=='1')):
            # If its non-future contract or a first futures contract
            p1 = self.prices[0]
            p2 = self.prices[1]
            if(p2!=0):
                logret = log(p1/p2)
            else:
                logret = 0  # If last two prices not available for a product,let logreturn = 0
            updated=True
        if(updated):
            self.values.append((self.dt[0].date(),logret))
            for listener in self.listeners: listener.on_daily_log_returns_update(self.product,self.values)
