import sys
from numpy import *
from Algorithm.TradeAlgorithm import TradeAlgorithm
from Utils.Regular import check_eod

class TargetRiskRPv1( TradeAlgorithm ):

    def init( self, _config ):
        self.day=-1
        self.target_risk = _config.getfloat( 'Strategy', 'target_risk' )
        self.rebalance_frequency = _config.getint( 'Parameters', 'rebalance_frequency' )
        self.indicator_mappings = {}
        _identifiers = _config.get( 'DailyIndicators', 'names' ).split(' ')
        for _identifier in _identifiers:
            _product = _identifier.split('.')[1]
            self.indicator_mappings[_product] = _identifier

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
            for product in self.products:
                target_risk_per_product = self.target_risk/len(self.products)
                risk = self.daily_indicators[ self.indicator_mappings[product] ].values[1] # Index 0 contains the date and 1 contains the value of indicator                               
                annualized_risk_of_product = ( exp( sqrt(252.0)*risk ) -1 )*100.0
                weights[product] = target_risk_per_product/annualized_risk_of_product 
            self.update_positions( events[0]['dt'], weights )
        else:
            self.rollover( events[0]['dt'] )