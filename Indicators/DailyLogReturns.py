from BookBuilder.BookBuilder_Listeners import DailyBookListener
from BookBuilder.BookBuilder import BookBuilder
from numpy import *
import datetime

##Track the daily log returns for the product
class DailyLogReturns(DailyBookListener):

    instance = []

    def __init__(self,products,config_file):
        self.prices={}
        self.dt={}
        self._DailyLogReturns={}                                                               #Track the daily log returns for the product
        self._yesterday_settlement={}                                                          #Track if yesterday was a settlement day,to update daily log returns correctly
        self.listeners=[]
        for product in products:
            self._DailyLogReturns[product] = empty(shape=(0))
            self._yesterday_settlement[product] = False
            self.prices[product]=[0,0]                                          #Remember last two prices for each product #prices[0] is latest
            self.dt[product]=datetime.datetime.fromtimestamp(1)
            bookbuilder = BookBuilder.GetUniqueInstance(product,config_file)
            bookbuilder.AddDailyBookListener(self)

    def AddListener(self,listener):
        self.listeners.append(listener)

    @staticmethod
    def GetUniqueInstance(products,config_file):
        if(len(DailyLogReturns.instance)==0):
            new_instance = DailyLogReturns(products,config_file)
            DailyLogReturns.instance.append(new_instance)
        return DailyLogReturns.instance[0]

    ##Update the daily log returns on each ENDOFDAY event
    def OnDailyBookUpdate(self,product,dailybook,is_settlement_day):
        self.dt[product]=dailybook[-1][0]
        self.prices[product][1]=self.prices[product][0]
        self.prices[product][0]=dailybook[-1][1]
        if(product[0]=='f' and self._yesterday_settlement[product] and (product[-1]=='1' or product[-1]=='2')):       #If its the first futures contract and yesterday was the settlement day
            product1 = product.rstrip('12')+'1'                                                  #Example : product1 = fES1
            product2 = product.rstrip('12')+'2'                                                  #Example : product2 = fES2
            p1 = self.prices[product1][0]
            p2 = self.prices[product2][1]
            if(self.dt[product1]==self.dt[product2] and p2!=0):
                logret = log(p1/p2)
                self._DailyLogReturns[product1] = append(self._DailyLogReturns[product1],logret)
                for listener in self.listeners: listener.OnDailyLogReturnsUpdate(product1,self._DailyLogReturns[product1])
        else:
            p1 = self.prices[product][0]
            p2 = self.prices[product][1]
            if(p2!=0):
                logret = log(p1/p2)
                self._DailyLogReturns[product] = append(self._DailyLogReturns[product],logret)
                for listener in self.listeners: listener.OnDailyLogReturnsUpdate(product,self._DailyLogReturns[product])
        self._yesterday_settlement[product]= is_settlement_day
