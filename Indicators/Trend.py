from numpy import *
import ConfigParser
from Indicator_Listeners import DailyLogReturnsListener
from DailyLogReturns import DailyLogReturns


# Track the direction of trend for various periods for all products
class Trend(DailyLogReturnsListener):

    instance=[]

    def __init__(self,products,config_file):
        self._Trend={}
        for product in products:
            self._Trend[product] = empty(shape=(0))
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))
        self.periods = config.get('Trend', 'periods').strip().split(",")
        self.periods = [int(i) for i in self.periods]
        self.listeners=[]
        daily_log_ret = DailyLogReturns.get_unique_instance(products,config_file)
        daily_log_ret.add_listener(self)

    def add_listener(self,listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(products,config_file):
        if(len(Trend.instance)==0):
            new_instance = Trend(products,config_file)
            Trend.instance.append(new_instance)
        return Trend.instance[0]

    # Udate the trend indicators on each ENDOFDAY event
    def on_daily_log_returns_update(self,product,daily_log_returns):
        self._Trend[product] = empty(shape=(0))  # Way to initialize numpy array of 0 size
        for period in self.periods:
            n=daily_log_returns.shape[0]
            _start_index = max(0,n-period)  # If sufficient lookback not available,use the available data only to compute indicator
            val = sign(sum(daily_log_returns[_start_index:n]))
            self._Trend[product]=append(self._Trend[product],val)
