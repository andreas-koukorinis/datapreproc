from numpy import *
from Indicator_Listeners import IndicatorListener
from DailyLogReturns import DailyLogReturns


# Track the expected value of log returns for the product
# In the config file this indicator will be specfied as : ExpectedReturns,product,period
class ExpectedReturns( IndicatorListener ):

    instances = {}

    def __init__( self, identifier, _startdate, _enddate, _config ):
        self.values = () # Tuple of the form (dt,value)
        self.identifier = identifier
        params = identifier.strip().split('.')
        self.product = params[1]
        self.period = int( params[2] )
        # To maintain running expected returns
        self.current_sum = 0.0
        self.current_num = 0.0
        self.listeners = []
        daily_log_ret = DailyLogReturns.get_unique_instance( 'DailyLogReturns.' + self.product, _startdate, _enddate, _config )
        daily_log_ret.add_listener( self )

    def add_listener( self, listener ):
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config):
        if identifier not in ExpectedReturns.instances.keys() :
            new_instance = ExpectedReturns( identifier, _startdate, _enddate, _config )
            ExpectedReturns.instances[identifier] = new_instance
        return ExpectedReturns.instances[identifier]

    # Do online update of the expected returns indicator on each ENDOFDAY event
    def on_indicator_update( self, identifier, daily_log_returns_dt ):
        n = len(daily_log_returns_dt)
        if n > self.period:
            self.current_sum =  self.current_sum - daily_log_returns_dt[n-self.period-1][1] + daily_log_returns_dt[n-1][1] 
            val = self.current_sum/self.current_num
        elif n < 2:
            val = 0.001 # Dummy value for insufficient lookback period(case where only 1 log return)
            if n == 1:
                self.current_sum = daily_log_returns_dt[n-1][1]
                self.current_num = 1
        else:
            self.current_sum = self.current_sum + daily_log_returns_dt[n-1][1]
            self.current_num += 1
            val = self.current_sum/self.current_num
        if isnan(val):
            print ("something wrong")
        self.values = ( daily_log_returns_dt[-1][0], val )
        for listener in self.listeners: 
            listener.on_indicator_update( self.identifier, self.values )
