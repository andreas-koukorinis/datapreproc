from numpy import *
import ConfigParser
from Indicator_Listeners import DailyLogReturnsListener
from DailyLogReturns import DailyLogReturns


# Track the standard deviation of log returns for the product
class StdDev(DailyLogReturnsListener):

    instance = []

    def __init__(self,products,config_file):
        self._StdDev={}
        for product in products:
            self._StdDev[product] = empty(shape=(0))
        config = ConfigParser.ConfigParser()         
        config.readfp(open(config_file,'r')) 
        self.periods = config.get('StdDev', 'periods').strip().split(",")
        self.periods = [int(i) for i in self.periods]  # Get periods for which we need to track the Std
        self.listeners=[]
        daily_log_ret = DailyLogReturns.get_unique_instance(products,config_file)
        daily_log_ret.add_listener(self)  

    def add_listener(self,listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(products,config_file):
        if(len(StdDev.instance)==0):
            new_instance = StdDev(products,config_file)
            StdDev.instance.append(new_instance)
        return StdDev.instance[0]

    # Update the standard deviation indicators on each ENDOFDAY event
    def on_daily_log_returns_update(self,product,daily_log_returns):
        self._StdDev[product] = empty(shape=(0))
        for period in self.periods:
            n=daily_log_returns.shape[0]
            _start_index = max(0,n-period)  # If sufficient lookback not available,use the available data only to compute indicator
            val = std(daily_log_returns[_start_index:n])
            if(val==0): 
                val=0.001  # Dummy value for insufficient lookback period(case where only 1 log return)
            self._StdDev[product]=append(self._StdDev[product],val)
