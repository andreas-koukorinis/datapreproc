from numpy import *
from Indicator_Listeners import IndicatorListener
from DailyLogReturns import DailyLogReturns
from portfolio_utils import get_products_from_portfolio_string

# Compute the correlation of log returns of a number of product for the specified number of days
class CorrelationLogReturns( IndicatorListener ):

    instances = {}

    def __init__( self, identifier, _startdate, _enddate, _config ):
        self.values = () # Tuple of the form (dt,value)
        self.identifier = identifier # The identifier will be of the form
        params = identifier.strip().split('.') # interpretation of params is PortfolioString and number of days to look back
        
        self.portfolio = params[1]
        self.products = get_products_from_portfolio_string ( self.portfolio )
        self.period = int( params[2] )
        self.listeners = [] # probably not very useful here since we don't expect to have listners to this piece of logic
        self.map_identifier_to_index = {} # this will help us know th column number wen we get on_indicator_update
        self.logret_matrix = [] # TODO, probably should be a list of lists, which we can later conver to a numpy array ?

        # create a diagonal matrix
        self.correlation_matrix = zeros ( len(self.products), len(self.products) )
        for i in xrange(len(self.products)):
            self.correlation_matrix[i,i] = 1.0

        self.correlation_matrix_already_computed = False
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
        self.correlation_matrix_already_computed = False # this just marks that we need to recompute elements
        
    # For performance reasons, perhaps we don't want to recompute this in on_indicator_update
    def recompute ():
        if not self.correlation_matrix_already_computed:
            # compute correlation matrix, covariance matrix, stdev array
            self.correlation_matrix_already_computed = True
        return (self.correlation_matrix) 
