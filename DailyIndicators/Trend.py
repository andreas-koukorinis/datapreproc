from numpy import *
from Indicator_Listeners import IndicatorListener
from DailyLogReturns import DailyLogReturns


# Track the direction of trend for the product
# In the config file this indicator will be specfied as : Trend,product,period
class Trend( IndicatorListener ):

    instances = {}

    def __init__( self, identifier, _startdate, _enddate, _config ):
        self.values = () # Tuple of the form (dt,value)
        self.identifier = identifier
        params = identifier.strip().split('.')
        self.product = params[1]
        self.period = float( params[2] )
        self.listeners = []
        daily_log_ret = DailyLogReturns.get_unique_instance( 'DailyLogReturns.' + self.product, _startdate, _enddate, _config )
        daily_log_ret.add_listener( self )

    def add_listener( self, listener ):
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config ):
        if identifier not in Trend.instances.keys() :
            new_instance = Trend ( identifier, _startdate, _enddate, _config )
            Trend.instances[identifier] = new_instance
        return Trend.instances[identifier]

    # Update the standard deviation indicators on each ENDOFDAY event
    def on_indicator_update( self, identifier, daily_log_returns_dt ):
        daily_log_returns = array( [ item[1] for item in daily_log_returns_dt ] ).astype( float )
        n = daily_log_returns.shape[0]
        _start_index = max( 0, n - self.period )  # If sufficient lookback not available,use the available data only to compute indicator
        val = sign( sum( daily_log_returns[ _start_index : n ] ) )
        if n < 1 : val = 0
        self.values = ( daily_log_returns_dt[-1][0], val )
        for listener in self.listeners: 
            listener.on_indicator_update( self.identifier, self.values )
