import sys
from numpy import *
from Algorithm.TradeAlgorithm import TradeAlgorithm
from Utils.Regular import check_eod
from DailyIndicators.Indicator_List import is_valid_daily_indicator
from DailyIndicators.correlation_log_returns import CorrelationLogReturns
form DailyIndicators.portfolio_utils import make_portfolio_string_from_products

class TargetRiskEqualRiskContribution(TradeAlgorithm):

    def init( self, _config ):
        self.day=-1 # TODO move this to "watch" or aglobal time manager
        self.target_risk = _config.getfloat( 'Strategy', 'target_risk' )
        self.rebalance_frequency = _config.getint( 'Parameters', 'rebalance_frequency' )
        self.stddev_computation_history = _config.getint( 'Strategy', 'stddev_computation_history' )
        self.stddev_computation_indicator = _config.get( 'Strategy', 'stddev_computation_indicator' )
        self.correlation_computation_history = _config.getint( 'Strategy', 'correlation_computation_history' )
        self.correlation_computation_interval = _config.getint( 'Strategy', 'correlation_computation_interval' )
        self.last_date_correlation_computed = 0
        self.stdev_computation_indicator_mapping = {}

        if ( is_valid_daily_indicator ( self.stddev_computation_indicator ) ):
            for product in self.products:
                _orig_indicator_name = self.stddev_computation_indicator + product + str(self.stddev_computation_history) #this would be something like StdDev.fTY.21
                module = import_module( 'DailyIndicators.' + stddev_computation_indicator )
                Indicatorclass = getattr( module, stddev_computation_indicator )
                self.daily_indicators[_orig_indicator_name] = Indicatorclass.get_unique_instance( _orig_indicator_name, self.start_date, self.end_date, _config )
                self.stddev_computation_indicator[product] = self.daily_indicators[_orig_indicator_name]

        _portfolio_string = make_portfolio_string_from_products ( self.products )
        self.correlation_computation_indicator = CorrelationLogReturns.get_unique_instance( _portfolio_string+'.'+str(self.correlation_computation_history) )

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
        if all_eod and self.day % self.rebalance_frequency == 0:
            if self.day > ( self.last_date_correlation_matrix_computed + self.correlation_computation_interval ):
                # we need to recompute the correlation matrix
                
            # Calculate weights to assign to each product using indicators
            weights = {}
            # TODO
            self.update_positions( events[0]['dt'], weights )
        else:
            self.rollover( events[0]['dt'] )
