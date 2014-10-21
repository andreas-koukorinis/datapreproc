import sys
from numpy import *
import ConfigParser
from Algorithm.TradeAlgorithm import TradeAlgorithm
from Utils.Regular import check_eod
from Utils.Calculate import get_worth,get_positions_from_weights

class UnleveredDMF(TradeAlgorithm):

    def init(self,config_file):
        self.Daily_Indicators = ['StdDev','Trend']  # Indicators will be updated in the same order as they are specified in the list
        self.Intraday_Indicators = []
        self.day=-1
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))
        self.rebalance_frequency=config.getint('Parameters', 'rebalance_frequency')

    '''  Use self.bb_objects[product].dailybook to access the closing prices for the 'product'
         Use self.bb_objects[product].intradaybook to access the intraday prices for the 'product'
         dailybook consists of tupes of the form (timestamp,closing prices) sorted by timestamp
         'events' is a list of concurrent events
         event = {'price': 100, 'product': 'ES1', 'type':'ENDOFDAY', 'dt': datetime(2005,1,2,23,59,99999), 'table': 'ES','is_last_trading_day':False}
         access conversion_factor using : self.conversion_factor['ES1'] '''

    def on_events_update(self,events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if(all_eod): self.day += 1   # Track the current day number

        current_portfolio = self.get_portfolio()  # Current_portfolio consists of : 'cash','num_shares','products'   
        positions_to_take = {}

        # Get the current price for each product
        current_price = {}
        for product in self.products:
            current_price[product] = self.bb_objects[product].dailybook[-1][1]
           
        # If today is the rebalancing day,then use indicators to calculate new positions to take
        if(all_eod and self.day%self.rebalance_frequency==0):

            # Calculate current worth
            current_worth = get_worth(current_price,self.conversion_factor,current_portfolio)  

            # Calculate weights to assign to each product using indicators
            weight = {}
            sum_weights=0
            for product in self.products:
                if(product[0]=='f' and product[-1]!='1'):  # Dont trade futures contracts other than the first futures contract
                    weight[product] = 0
                else:
                    risk = self.daily_indicators['StdDev']._StdDev[product][0]  # 1 month Std
                    trend = self.daily_indicators['Trend']._Trend[product][0]
                    weight[product] = trend/risk
                sum_weights = sum_weights+abs(weight[product])
            for product in self.products: 
                weight[product]=weight[product]/sum_weights

            # Calculate positions from weights
            positions_to_take = get_positions_from_weights(weight,current_worth,current_price,self.conversion_factor)   
           
        # Otherwise positions_to_take is same as current portfolio composition
        else:
            for product in self.products:
                positions_to_take[product] = current_portfolio['num_shares'][product]
 
        # Adjust positions for settlement day
        positions_to_take = self.adjust_positions_for_settlements(current_price,positions_to_take)

        # Place orders.Since all events are concurrent, the datetime attribute of all the events will be same
        for product in self.products:
                self.place_order_target(events[0]['dt'],product,positions_to_take[product])
