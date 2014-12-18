import sys
from numpy import *
from Algorithm.trade_algorithm import TradeAlgorithm
from Utils.Regular import check_eod

class CTAMomentum( TradeAlgorithm ):

    def init( self, _config ):
        self.day=-1
        self.rebalance_frequency = _config.getint( 'Parameters', 'rebalance_frequency' )

    '''  Use self.bb_objects[product].dailybook to access the closing prices for the 'product'
         Use self.bb_objects[product].intradaybook to access the intraday prices for the 'product'
         dailybook consists of tupes of the form (timestamp,closing prices,is_last_trading_day) sorted by timestamp
         'events' is a list of concurrent events
         event = {'price': 100, 'product': 'ES1', 'type':'ENDOFDAY', 'dt': datetime(2005,1,2,23,59,99999), 'table': 'ES','is_last_trading_day':False}
         access conversion_factor using : self.conversion_factor['ES1']'''

    def on_events_update(self,events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number
           
        # If today is the rebalancing day,then use indicators to calculate new positions to take
        if all_eod and self.day % self.rebalance_frequency == 0 :
            # Calculate weights to assign to each product using indicators
            weights = {}
            sum_weights = 0.0
            for product in self.products:
                ma_short = self.daily_indicators[ 'MovingAverage1.' + product + '.50' ].values[1] # Index 0 contains the date and 1 contains the value of indicator                               
                ma_long = self.daily_indicators[ 'MovingAverage1.' + product + '.200' ].values[1] # Index 0 contains the date and 1 contains the value of indicator                               
#                annualized_risk_of_product = ( exp( sqrt(252.0)*risk ) -1 )*100.0
                if ma_short >=ma_long:           
                    weights[product] = 1
                else:
                    weights[product] = -1
                    
                sum_weights = sum_weights + abs(weights[product]) # Here abs does not make any difference,but in general should do
            for product in self.products:
                weights[product] = weights[product]/sum_weights                
            self.update_positions( events[0]['dt'], weights )
        else:
            self.rollover( events[0]['dt'] )
