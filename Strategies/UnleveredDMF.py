import sys
from numpy import *
from Algorithm.TradeAlgorithm import TradeAlgorithm
from Utils.Regular import check_eod

class UnleveredDMF( TradeAlgorithm ):

    def init( self, _config ):
        self.day=-1
        self.rebalance_frequency = _config.getint( 'Parameters', 'rebalance_frequency' )

    def process_model_file(self, _modelfilepath):
        _model_file_handle = open( _modelfilepath, "r" )
        for _model_line in _model_file_handle:
            # Default StdDevIndicator StdDev
            # Default StdDevComputationHistory 63
            # Default StdDevComputationInterval 5
            # Default TrendIndicator Trend
            # Default TrendComputationParameters 21 5 63 5 252 5
            # fES TrendComputationParameters 63 5 252 5


    def on_events_update(self,events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number
           
        # If today is the rebalancing day,then use indicators to calculate new positions to take
        if all_eod and self.day % self.rebalance_frequency == 0 :
            # Calculate weights to assign to each product using indicators
            weights = {}
            sum_weights = 0.0
            for product in self.products:
                risk = self.daily_indicators[ 'StdDev.' + product + '.21' ].values[1] # Index 0 contains the date and 1 contains the value of indicator                               
                trend = self.daily_indicators[ 'Trend.' + product + '.5' ].values[1]
                weights[product] = trend/risk
                sum_weights = sum_weights + abs( weights[product] ) 
            for product in self.products:
                weights[product] = weights[product]/sum_weights                
            self.update_positions( events[0]['dt'], weights )
        else:
            self.rollover( events[0]['dt'] )
