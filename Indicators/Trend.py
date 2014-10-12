from Indicator_Listeners import DailyLogReturnsListener
from DailyLogReturns import DailyLogReturns
from numpy import *
import ConfigParser

##Track the direction of trend for various periods for all products
class Trend(DailyLogReturnsListener):

    instance=[]

    def __init__(self,products,config_file):
        self._Trend={}                        
        for product in products:
            self._Trend[product] = empty(shape=(0))
        config = ConfigParser.ConfigParser()                                                  #Get config file handler
        config.readfp(open(config_file,'r')) 
        self.periods = config.get('Trend', 'periods').strip().split(",")
        self.periods = [int(i) for i in self.periods] 
        self.listeners=[]
        dailylogret = DailyLogReturns.get_unique_instance(products,config_file)
        dailylogret.AddListener(self)                                                     #Add as a listener to DailyLogReturns Indicator

    def AddListener(self,listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(products,config_file):
        if(len(Trend.instance)==0):
            new_instance = Trend(products,config_file)
            Trend.instance.append(new_instance)
        return Trend.instance[0]
    
    ##Udate the trend indicators on each ENDOFDAY event
    def OnDailyLogReturnsUpdate(self,product,DailyLogReturns):
        self._Trend[product] = empty(shape=(0)) # way to initialize numpy array of 0 size
        for period in self.periods:
            n=DailyLogReturns.shape[0]
            if(n>=period): 
                val = sign(sum(DailyLogReturns[n-period:n]))
                self._Trend[product]=append(self._Trend[product],val)
            else:
                self._Trend[product]=append(self._Trend[product],nan)
