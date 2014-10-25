from numpy import *
import datetime
from BookBuilder.BookBuilder_Listeners import DailyBookListener
from BookBuilder.BookBuilder import BookBuilder

# Track the daily log returns for the product
class DailyLogReturns(DailyBookListener):

    instances = {}

    def __init__(self,identifier, _startdate, _enddate, config_file):
        self.listeners=[]
        self.values = []
        self.prices=[0,0]  # Remember last two prices for the product #prices[0] is latest
        self.dt=[datetime.datetime.fromtimestamp(1),datetime.datetime.fromtimestamp(1)] # Last update dt for futures pair
        self.identifier=identifier
        params=identifier.strip().split('.')
        self.product=params[1]
        bookbuilder = BookBuilder.get_unique_instance ( self.product, _startdate, _enddate, config_file )
        bookbuilder.add_dailybook_listener(self)
        if(self.product[0]=='f' and self.product[-1]=='1'):
            self.price2=0 # Remember the last price for the 2nd future contract if this is the first futures contract
            product2 = self.product.rstrip('1')+'2'
            bookbuilder = BookBuilder.get_unique_instance(product2, _startdate, _enddate, config_file)
            bookbuilder.add_dailybook_listener(self)

    def add_listener(self,listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(identifier, _startdate, _enddate, config_file):
        if(identifier not in DailyLogReturns.instances.keys()):
            new_instance = DailyLogReturns ( identifier, _startdate, _enddate, config_file )
            DailyLogReturns.instances[identifier] = new_instance
        return DailyLogReturns.instances[identifier]

    # Update the daily log returns on each ENDOFDAY event
    def on_dailybook_update(self,product,dailybook):
        updated=False
        if(self.product[0]=='f' and self.product[-1]=='1'):
            if(product[-1]=='1'): # For first futures contract update 0 index of dt
                self.dt[0]=dailybook[-1][0]
                self.prices[1]=self.prices[0]
                self.prices[0]=dailybook[-1][1]
            else:
                self.dt[1]=dailybook[-1][0] # For non-first(second) future contracts,update 1st index of dt
                self.price2=dailybook[-1][1]

            if(len(dailybook)>1):
                _yesterday_settlement = dailybook[-2][2]
            else:
                _yesterday_settlement = False

            if(_yesterday_settlement and self.dt[0]==self.dt[1]):
            # If yesterday was the settlement day and price for both 1st and 2nd contract has been updated
                product1 = product.rstrip('12')+'1'  # Example : product1 = fES1
                product2 = product.rstrip('12')+'2'  # Example : product2 = fES2
                p1 = self.prices[0]
                p2 = self.price2
                updated=True
            elif(not _yesterday_settlement and product[-1]=='1'):
                p1 = self.prices[0]
                p2 = self.prices[1]
                updated=True

        elif(self.product==product):
            self.dt[0]=dailybook[-1][0]
            self.prices[1]=self.prices[0]
            self.prices[0]=dailybook[-1][1]
            p1 = self.prices[0]
            p2 = self.prices[1]
            updated=True

        if(updated):
            if(p2!=0):
                logret = log(p1/p2)
            else:
                logret = 0  # If last two prices not available for a product,let logreturn = 0
            self.values.append((self.dt[0].date(),logret))
            for listener in self.listeners: listener.on_indicator_update(self.identifier,self.values)
