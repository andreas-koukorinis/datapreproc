import sys
import numpy as numpy
from Utils.Regular import check_eod
from DailyIndicators.Indicator_List import is_valid_daily_indicator
from DailyIndicators.portfolio_utils import make_portfolio_string_from_products
from Algorithm.TradeAlgorithm import TradeAlgorithm

class UnleveredDMF( TradeAlgorithm ):
    """Implement a momentum strategy on multiple products.
    The estimated return on each product is a sum of the discretized returns in the past durations.
    For instance if we are provided the TrendIndicator as Trend
    and TrendComputationParameters for fES: 63 252 5
    We will interpret this as we need to create two instances of Trend indicator with arguments 63 and 252
    and recompute them every 5 days.
    On every recomputation day, we will take the sign of the values of the indicators.
    We will sum them up, and divide by the maximum positive score.
    Hence we have a normalized expected return for each product.
    We have an estimate of risk, StdDev for each product.
    Based on Kelly Breiman bet sizing, we aim to take a position propotional to the ( expected return / expected risk )
    in each prodct. These weights are normalized to have a full capital usage at all times in this version,
    hence the name Unlevered.
    """
    
    def init( self, _config ):
        self.day=-1
        self.rebalance_frequency = 1
        if _config.has_option('Parameters', 'rebalance_frequency'):
            self.rebalance_frequency = _config.getint('Parameters', 'rebalance_frequency')
        self.StdDevIndicator="AverageStdDev"
        self.StdDevComputationParameters="63 252 5"
        self.TrendIndicator="AverageTrend"
        self.TrendComputationParameters="21 63 252 5"

    def process_model_file(self, _modelfilepath):
        _model_file_handle = open( _modelfilepath, "r" )
        for _model_line in _model_file_handle:
            # We expect lines like:
            # Default StdDevIndicator StdDev
            # Default StdDevComputationParameters 63 5
            # Default TrendIndicator Trend
            # Default TrendComputationParameters 21 63 252 5
            # fES TrendComputationParameters 63 252 5


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
