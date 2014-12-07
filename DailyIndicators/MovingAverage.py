from numpy import *
from Indicator_Listeners import IndicatorListener
from BookBuilder.BookBuilder import BookBuilder
from BookBuilder.BookBuilder_Listeners import DailyBookListener
from Utils.Regular import get_first_futures_contract,is_future_entity

# Track the standard deviation of log returns for the product
# In the config file this indicator will be specfied as : MovingAverage1.product.period
class MovingAverage( DailyBookListener ):

    instances = {}

    def __init__( self, identifier, _startdate, _enddate, _config ):
        self.values = () # Tuple of the form (dt,value)
        self.identifier = identifier
        params = identifier.strip().split('.')
        _product = params[1]
        if is_future_entity( _product ): # for a  future like fES, we want the product to be the first contract i.e. fES_1
            _product = get_first_futures_contract( _product )
        self.product = _product
        self.period = int( params[2] )
        self.current_sum = 0.0
        self.current_num = 0.0
        self.listeners = []
        BookBuilder.get_unique_instance( self.product, _startdate, _enddate, _config ).add_dailybook_listener( self )

    def add_listener( self, listener ):
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config):
        if identifier not in MovingAverage.instances.keys() :
            new_instance = MovingAverage( identifier, _startdate, _enddate, _config )
            MovingAverage.instances[identifier] = new_instance
        return MovingAverage.instances[identifier]

    # Update moving average indicators on each ENDOFDAY event
    def on_dailybook_update( self, product, dailybook ):
        n = len(dailybook)
        if n > self.period:
            self.current_sum = self.current_sum - dailybook[n-self.period-1][1] + dailybook[n-1][1] # dailybook[k][1] is the kth closing price in dailybook
            val = self.current_sum/self.current_num # The mean of closing prices
        elif n < 1:
            val = 0.001 # Dummy value for insufficient lookback period(case where only 1 log return)
        else:
            self.current_sum = self.current_sum + dailybook[n-1][1]
            self.current_num += 1
            val = self.current_sum/self.current_num # The mean of closing prices
        self.values = ( dailybook[-1][0], val )         
        for listener in self.listeners: 
            listener.on_indicator_update( self.identifier, self.values )
