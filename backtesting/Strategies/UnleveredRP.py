import sys
from numpy import *
from Algorithm.TradeAlgorithm import TradeAlgorithm
from Utils.Regular import checkEOD
from Utils.Calculate import get_worth,get_positions_from_weights
import ConfigParser

class UnleveredRP(TradeAlgorithm):

    instance=[]

    def init(self,config_file):
        self.daily_indicators = ['StdDev']                                                     #Indicators will be updated in the same order as they are specified in the list
        self.intraday_indicators = []
        self.day=-1
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))
        self.rebalance_frequency=config.getint('Parameters', 'rebalance_frequency')

#------------------------------------------------------------------------INDICATORS---------------------------------------------------------------------------------------#
    # Use self.bb_objects[product].dailybook to access the closing prices for the 'product'
    # Use self.bb_objects[product].intradaybook to access the intraday prices for the 'product'
    # dailybook consists of tupes of the form (timestamp,closing prices) sorted by timestamp
    # 'events' is a list of concurrent events
    # event = {'price': 100, 'product': 'ES1', 'type':'ENDOFDAY', 'dt': datetime(2005,1,2,23,59,99999), 'table': 'ES','is_settlement_day':False}
    # access conversion_factor using : self.conversion_factor['ES1']
    #To access the daily indicators.EG:StdDev : self.Daily_Indicators['StdDev']._StdDev[product] will contain the list of updated daily log return standard deviations for each period specified in the config file in the same order
    def OnEventsUpdate(self,events):
        all_EOD = checkEOD(events)								#check whether all the events are ENDOFDAY
        if(all_EOD): self.day += 1                                                              #Track the current day number

        current_portfolio = self.get_portfolio()                                                #current_portfolio consists of : 'cash','num_shares','products'   
        positions_to_take = {}

        #Get the current price for each product
        current_price = {}
        for product in self.products:
            current_price[product] = self.bb_objects[product].dailybook[-1][1] # change this to read access
           
        #If today is the rebalancing day,then use indicators to calculate new positions to take
        if(all_EOD and self.day%self.rebalance_frequency==0):

            #calculate current worth
            current_worth = get_worth(current_price,self.conversion_factor,current_portfolio)  

            #Calculate weights to assign to each product using indicators
            weight = {}
            sum_weights=0
            for product in self.products:
                if(product[0]=='f' and product[-1]!='1'):		#Dont trade futures contracts other than the first futures contract
                    weight[product] = 0
                else:
                    risk = (self.Daily_Indicators['StdDev']._StdDev[product][0]+self.Daily_Indicators['StdDev']._StdDev[product][1])/2             #average of 1 month and 2month Std
                    weight[product] = 1/risk
                sum_weights = sum_weights+abs(weight[product])
            for product in self.products: 
                weight[product]=weight[product]/sum_weights

            #Calculate positions from weights
            #Assumption: Use 95% of the wealth to decide positions,rest 5% for costs and price changes
            positions_to_take = get_positions_from_weights(weight,current_worth*0.95,current_price,self.conversion_factor)   
           
        # Otherwise positions_to_take is same as current portfolio composition
        else:
            for product in self.products:
                positions_to_take[product] = current_portfolio['num_shares'][product]
 
        # Adjust positions for settlement day
        positions_to_take = self.adjust_positions_for_settlements(events,current_price,positions_to_take)

        # Place orders.Since all events are concurrent, the datetime attribute of all the events will be same
        for product in self.products:
            self.place_order_target(events[0]['dt'],product,positions_to_take[product])
