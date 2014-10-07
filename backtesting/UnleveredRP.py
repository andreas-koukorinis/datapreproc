import sys
from TradeAlgorithm import TradeAlgorithm
from numpy import *
from Utils import checkEOD,get_worth,get_positions_from_weights

class UnleveredRP(TradeAlgorithm):

    def init(self):
        self.daily_indicators = ['DailyLogReturns','StdDev21']                                  #Indicators will be updated in the same order as they are specified in the list
        self.intraday_indicators = []
        
        #Initialize space for daily indicators
        self._DailyLogReturns = {}
        self._StdDev21 = {}
        self._yesterday_settlement = {}
        for product in self.products:
            self._DailyLogReturns[product]= empty(shape=(0))                                    #Track the daily log returns for the product
            self._StdDev21[product]=0								#Track the last month's standard deviation of daily log returns for the product
            self._yesterday_settlement[product]=False                                           #Track if yesterday was a settlement day,to update daily log returns correctly

        self.day=-1
        self.maxentries_dailybook = 200
        self.maxentries_intradaybook = 200
        self.rebalance_frequency=5
        self.warmupdays = 21									#number of days needed for the lookback of the strategy

#------------------------------------------------------------------------INDICATORS---------------------------------------------------------------------------------------#
#Use self.bb_objects[product].dailybook to access the closing prices for the 'product'
#Use self.bb_objects[product].intradaybook to access the intraday prices for the 'product'
#dailybook consists of tupes of the form (timestamp,closing prices) sorted by timestamp

    ##Track the daily log returns for the product
    def DailyLogReturns(self,product,is_settlement_day): 
        if(product[0]=='f' and self._yesterday_settlement[product] and product[-1]=='1'):       #If its the first futures contract and yesterday was the settlement day
            product1 = product                                                                  #Example : product1 = fES1
            product2 = product.rstrip('0123456789')+'2'                                         #Example : product2 = fES2
            dailybook1 = self.bb_objects[product1].dailybook
            dailybook2 = self.bb_objects[product2].dailybook
            n1 = len(dailybook1)
            n2 = len(dailybook2)
            if(n1>=1 and n2>=2):
                logret = log(dailybook1[-1][1]/dailybook2[-2][1])
                self._DailyLogReturns[product] = append(self._DailyLogReturns[product],logret)
        else:
            dailybook = self.bb_objects[product].dailybook
            n = len(dailybook)
            if(n>=2):
                logret = log(dailybook[-1][1]/dailybook[-2][1])
                self._DailyLogReturns[product] = append(self._DailyLogReturns[product],logret)             
        self._yesterday_settlement[product]= is_settlement_day
         
    #Track the last month's standard deviation of daily log returns for the product
    def StdDev21(self,product,is_settlement_day):
        n = self._DailyLogReturns[product].shape[0]
        k = 21
        if(n-k<0): return									#If lookback period not sufficient,dont calculate the indicator
        self._StdDev21[product] = std(self._DailyLogReturns[product][n-k:n])

#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #'events' is a list of concurrent events
    # event = {'price': 100, 'product': 'ES1', 'type':'ENDOFDAY', 'dt': datetime(2005,1,2,23,59,99999), 'table': 'ES','is_settlement_day':False}
    # access conversion_factor using : self.conversion_factor['ES1']

    def OnEventListener(self,events):
        all_EOD = checkEOD(events)								#check whether all the events are ENDOFDAY
        if(all_EOD): self.day += 1                                                              #Track the current day number

        current_portfolio = self.portfolio.get_portfolio()                                      #current_portfolio consists of : 'cash','num_shares','products'   
        positions_to_take = {}

        #Get the current price for each product
        current_price = {}
        for product in self.products:
            current_price[product] = self.bb_objects[product].dailybook[-1][1]
           
        #If today is the rebalancing day,then use indicators to calculate new positions to take
        if(all_EOD and self.day%self.rebalance_frequency==0):

            #calculate current worth
            current_worth = get_worth(current_price,self.conversion_factor,current_portfolio)  

            #Calculate weights to assign to each product using indicators
            weight = {}
            sum_weights=0
            for product in self.products:
                if(product[0]=='f' and product[-1]!='1'):					#Dont trade futures contracts other than the first futures contract
                    weight[product] = 0
                else:
                    weight[product] = 1/self._StdDev21[product]
                sum_weights = sum_weights+weight[product]
            for product in self.products: 
                weight[product]=weight[product]/sum_weights

            #Calculate positions from weights
            #Assumption: Use 95% of the wealth to decide positions,rest 5% for costs and price changes
            positions_to_take = get_positions_from_weights(weight,current_worth*0.95,current_price,self.conversion_factor)   
           
        #Otherwise positions_to_take is same as current portfolio composition
        else:
            for product in self.products:
                positions_to_take[product] = current_portfolio['num_shares'][product]
 
        #Adjust positions for settlement day
        positions_to_take = self.adjust_positions_for_settlements(events,current_price,positions_to_take)

        #Place orders.Since all events are concurrent, the datetime attribute of all the events will be same
        for product in self.products:
                self.place_order_target(events[0]['dt'],product,positions_to_take[product])
