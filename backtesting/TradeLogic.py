import sys
from TradeAlgorithm import TradeAlgorithm
from numpy import *

class TradeLogic(TradeAlgorithm):

    def init(self):
        self.daily_indicators = ['StdDev']
        self.intraday_indicators = []
        self.indicator_values = {}
        self.day=0
        self.maxentries_dailybook = 200
        self.maxentries_intradaybook = 200
        self.rebalance_frequency=20
        self.warmupdays = 63									#number of days needed for the lookback of the strategy

    #dailybook consists of tupes of the form (timestamp,closing prices) sorted by timestamp
    def StdDev(self,dailybook,product):
        dates  = [i[0] for i in dailybook]
        prices = [i[1] for i in dailybook]
        prices = array(prices).astype(float)
        n = prices.shape[0]
        returns = log(prices[1:n-1]/prices[0:n-2])
        n=n-1
        k = 63								                        #maintain standard deviation of returns for the past 3 months
        return std(returns[n-k-1:n-1])

    #'events' is a list of concurrent events
    # event = {'price': 100, 'product': 'ES1', 'type':'ENDOFDAY', 'dt': datetime(2005,1,2,23,59,99999), 'table': 'ES'}
    # access conversion_factor using : self.conversion_factor['ES1']
    ##########RISK PARITY UNLEVERED###########
    def OnEventListener(self,events):
        if(events[0]['type']=='ENDOFDAY'):
            self.day +=1
        if(self.day%self.rebalance_frequency==0):
            self.place_order(events[0]['dt'],'ES1',10)                                              #place an order to buy 10 shares of product 1 on every rebalance_freq^th day

