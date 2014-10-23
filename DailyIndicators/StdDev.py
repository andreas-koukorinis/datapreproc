from numpy import *
import ConfigParser
from Indicator_Listeners import IndicatorListener
from DailyLogReturns import DailyLogReturns


# Track the standard deviation of log returns for the product
# In the config file this indicator will be specfied as : StdDev,product,period
class StdDev(IndicatorListener):

    instances = {}

    def __init__(self,identifier,config_file):
        self.values=() # Tuple of the form (dt,value)
        self.identifier=identifier
        params = identifier.strip().split('.')
        self.product= params[1]
        self.period = float(params[2])
        self.listeners=[]
        daily_log_ret = DailyLogReturns.get_unique_instance('DailyLogReturns.'+self.product,config_file)
        daily_log_ret.add_listener(self)  

    def add_listener(self,listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(identifier,config_file):
        if(identifier not in StdDev.instances.keys()):
            new_instance = StdDev(identifier,config_file)
            StdDev.instances[identifier]=new_instance
        return StdDev.instances[identifier]

    # Update the standard deviation indicators on each ENDOFDAY event
    def on_indicator_update(self,identifier,daily_log_returns_dt):
        daily_log_returns = array([item[1] for item in daily_log_returns_dt]).astype(float)
        n=daily_log_returns.shape[0]
        _start_index = max( 0, n - self.period )  # If sufficient lookback not available,use the available data only to compute indicator
        val = std(daily_log_returns[_start_index:n])
        if(n < 2 or val==0): 
            val=0.001  # Dummy value for insufficient lookback period(case where only 1 log return)
        self.values=(daily_log_returns_dt[-1][0],val)
        for listener in self.listeners: listener.on_indicator_update(self.identifier,self.values)
        
