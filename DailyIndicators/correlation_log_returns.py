from numpy import *
from Indicator_Listeners import IndicatorListener
from DailyLogReturns import DailyLogReturns
from portfolio_utils import get_products_from_portfolio_string

# Compute the correlation of log returns of a number of product for the specified number of days
class CorrelationLogReturns( IndicatorListener ):

    instances = {}

    def __init__( self, identifier, _startdate, _enddate, _config ):
        self.values = () # Tuple of the form (dt,value)
        self.identifier = identifier
        params = identifier.strip().split('.') # interpretation of params is PortfolioString and number of days to look back
        
        self.portfolio = params[1]
        self.products = get_products_from_portfolio_string ( self.portfolio )
        self.period = int( params[2] )
        self.listeners = []
        self.map_identifier_to_index = {}
        self.
        _index = 1 #later used for column to store returns in
        for product in self.products:
            _this_identifier='DailyLogReturns.' + self.product
            daily_log_ret = DailyLogReturns.get_unique_instance( _this_identifier, _startdate, _enddate, _config )
            daily_log_ret.add_listener( self )
            self.map_identifier_to_index[_this_identifier]=_index
            _index = _index + 1

    def add_listener( self, listener ):
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config ):
        # TODO ... this should be agnostic of order of the products in the portfolio string
        if identifier not in CorrelationLogReturns.instances.keys() :
            new_instance = CorrelationLogReturns ( identifier, _startdate, _enddate, _config )
            CorrelationLogReturns.instances[identifier] = new_instance
        return CorrelationLogReturns.instances[identifier]

    # Update the standard deviation indicators on each ENDOFDAY event
    def on_indicator_update( self, identifier, daily_log_returns_dt ):
        # TODO shoudl we check for the identifer or assume it is in the map ?
        
