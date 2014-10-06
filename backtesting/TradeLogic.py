import sys
from TradeAlgorithm import TradeAlgorithm
from numpy import *

class TradeLogic(TradeAlgorithm):

    def init(self):
        self.daily_indicators = ['DailyLogReturns','StdDev21']                                 #Indicators will be updated in the same order as they are specified in the list
        self.intraday_indicators = []
        
        #Initialize space for daily indicators
        self._DailyLogReturns = {}
        self._StdDev21 = {}
        self._yesterday_settlement = {}
        for product in self.products:
            self._DailyLogReturns[product]= empty(shape=(0))                                     #Track the daily log returns for the product
            self._StdDev21[product]=0								#Track the last month's standard deviation of daily log returns for the product
            self._yesterday_settlement[product]=False

        self.day=0
        self.maxentries_dailybook = 200
        self.maxentries_intradaybook = 200
        self.rebalance_frequency=20
        self.warmupdays = 63									#number of days needed for the lookback of the strategy

#------------------------------------------------------------------------INDICATORS---------------------------------------------------------------------------------------#
#Use self.bb_objects[product].dailybook to access the closing prices for the 'product'
#Use self.bb_objects[product].intradaybook to access the intraday prices for the 'product'
#dailybook consists of tupes of the form (timestamp,closing prices) sorted by timestamp

    ##Track the daily log returns for the product
    def DailyLogReturns(self,product,is_settlement_day): 
        if(product[0]=='f' and self._yesterday_settlement[product] and product[-1]=='1'):       #If its the first futures contract and yesterday was the settlement day
            product1 = product                                                                 #Example : product1 = fES1
            product2 = product.rstrip('0123456789')+'2'                                        #Example : product2 = fES2
            dailybook1 = self.bb_objects[product1].dailybook
            dailybook2 = self.bb_objects[product2].dailybook
            n1 = len(dailybook1)
            n2 = len(dailybook2)
            if(n1>=1 and n2>=2):
                logret = log(dailybook1[n1-1][1]/dailybook2[n2-2][1])
                self._DailyLogReturns[product] = append(self._DailyLogReturns[product],logret)
        else:
            dailybook = self.bb_objects[product].dailybook
            n = len(dailybook)
            if(n>=2):
                logret = log(dailybook[n-1][1]/dailybook[n-2][1])
                self._DailyLogReturns[product] = append(self._DailyLogReturns[product],logret)             
        self._yesterday_settlement[product]= is_settlement_day
         
    #Track the last month's standard deviation of daily log returns for the product
    def StdDev21(self,product,is_settlement_day):
        n = self._DailyLogReturns[product].shape[0]
        k = 21
        if(n-k-1<0): return									#If lookback period not sufficient,dont calculate the indicator
        self._StdDev21[product] = std(self._DailyLogReturns[product][n-1-k:n-1])

#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #'events' is a list of concurrent events
    # event = {'price': 100, 'product': 'ES1', 'type':'ENDOFDAY', 'dt': datetime(2005,1,2,23,59,99999), 'table': 'ES'}
    # access conversion_factor using : self.conversion_factor['ES1']
    ##########RISK PARITY UNLEVERED###########
    def OnEventListener(self,events):
        if(events[0]['type']=='ENDOFDAY'):
            self.day +=1
        if(self.day%self.rebalance_frequency==0):
            self.place_order(events[0]['dt'],events[0]['product'],10)                                              #place an order to buy 10 shares of product 1 on every rebalance_freq^th day

