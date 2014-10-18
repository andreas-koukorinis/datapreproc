from numpy import *
import ConfigParser
from Indicator_Listeners import DailyLogReturnsListener
from DailyLogReturns import DailyLogReturns


# Track the standard deviation of log returns for the product
class Trend(DailyLogReturnsListener):

    instances = {} # For every (product,period) pair keep 1 instance

    def __init__(self,product,params,config_file):
        self.values = [] # List of tuples (dates,values) 
        self.product = product
        self.period = params[0]
        self.listeners=[]
        daily_log_ret = DailyLogReturns.get_unique_instance(product,config_file)
        daily_log_ret.add_listener(self)  

    def add_listener(self,listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(product,params,config_file):
        key = str((product,params))
        if(key not in Trend.instances.keys()):
            new_instance = Trend(product,params,config_file)
            Trend.instances[key] = new_instance
        return Trend.instances[key]

    # Update the standard deviation indicators on each ENDOFDAY event
    def on_daily_log_returns_update(self,product,daily_log_returns_dt):
            daily_log_returns = array([i[1] for i in daily_log_returns_dt]).astype(float) # Convert to numpy array after removing dt
            n=daily_log_returns.shape[0]
            _start_index = max(0,n-self.period)  # If sufficient lookback not available,use the available data only to compute indicator
            val = sign(sum(daily_log_returns[_start_index:n]))
            date=daily_log_returns_dt[-1][0]
            self.values.append((date,val))
