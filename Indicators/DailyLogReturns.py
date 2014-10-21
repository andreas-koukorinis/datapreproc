from numpy import *
import datetime
from BookBuilder.BookBuilder_Listeners import DailyBookListener
from BookBuilder.BookBuilder import BookBuilder

# Track the daily log returns for the product
class DailyLogReturns(DailyBookListener):

    instance = []

    def __init__(self,products,config_file):
        self.prices={}
        self.dt={}
        self._daily_log_returns={}  # Store the daily log returns for the product
        self._yesterday_settlement={}  # Track if yesterday was a settlement day,to update daily log returns correctly
        self.listeners=[]
        for product in products:
            self._daily_log_returns[product] = empty(shape=(0))
            self._yesterday_settlement[product] = False
            self.prices[product]=[0,0]  # Remember last two prices for each product #prices[0] is latest
            self.dt[product]=datetime.datetime.fromtimestamp(1)
            bookbuilder = BookBuilder.get_unique_instance(product,config_file)
            bookbuilder.add_dailybook_listener(self)

    def add_listener(self,listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(products,config_file):
        if(len(DailyLogReturns.instance)==0):
            new_instance = DailyLogReturns(products,config_file)
            DailyLogReturns.instance.append(new_instance)
        return DailyLogReturns.instance[0]

    # Update the daily log returns on each ENDOFDAY event
    def on_dailybook_update(self,product,dailybook):
        is_last_trading_day=dailybook[-1][2]
        self.dt[product]=dailybook[-1][0]
        self.prices[product][1]=self.prices[product][0]
        self.prices[product][0]=dailybook[-1][1]
        if(product[0]=='f' and self._yesterday_settlement[product] and (product[-1]=='1' or product[-1]=='2')):  # If its the first futures contract and yesterday was the settlement day
            product1 = product.rstrip('12')+'1'  # Example : product1 = fES1
            product2 = product.rstrip('12')+'2'  # Example : product2 = fES2
            p1 = self.prices[product1][0]
            p2 = self.prices[product2][1]
            if(self.dt[product1]==self.dt[product2]):
                if(p2!=0):
                    logret = log(p1/p2)
                else:
                    logret = 0  # If last two prices not available for a product,let logreturn = 0
                self._daily_log_returns[product1] = append(self._daily_log_returns[product1],logret)
                for listener in self.listeners: listener.on_daily_log_returns_update(product1,self._daily_log_returns[product1])
        else:
            p1 = self.prices[product][0]
            p2 = self.prices[product][1]
            if(p2!=0):
                logret = log(p1/p2)
            else:
                logret = 0  # If last two prices not available for a product,let logreturn = 0
            self._daily_log_returns[product] = append(self._daily_log_returns[product],logret)
            for listener in self.listeners: listener.on_daily_log_returns_update(product,self._daily_log_returns[product])
        self._yesterday_settlement[product]= is_last_trading_day
