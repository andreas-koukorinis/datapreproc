# cython: profile=True
import sys
import math
from indicator_listeners import IndicatorListener
from daily_log_returns import DailyLogReturns
from utils.global_variables import Globals

# Track the expected value of log returns for the product
# In the config file this indicator will be specfied as : ExpectedReturns,product,period
class ExpectedReturns( IndicatorListener ):

    def __init__( self, identifier, _startdate, _enddate, _config ):
        self.values = () # Tuple of the form (dt,value)
        self.identifier = identifier
        params = identifier.strip().split('.')
        if len(params) <= 2:
            print("ExpectedReturns requires at least three parameters in the identifier, like ExpectedReturns.fES.63");
            sys.exit(0)
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
    def get_unique_instance(identifier, _startdate, _enddate, _config):
        if identifier not in Globals.expected_returns_instances.keys() :
            new_instance = ExpectedReturns( identifier, _startdate, _enddate, _config )
            Globals.expected_returns_instances[identifier] = new_instance
        return Globals.expected_returns_instances[identifier]

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
        if math.isnan(val):
            print ("something wrong")
        self.values = ( daily_log_returns_dt[-1][0], val )
        for listener in self.listeners: 
            listener.on_indicator_update( self.identifier, self.values )
