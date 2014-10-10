from Indicator_Listeners import DailyLogReturnsListener
from DailyLogReturns import DailyLogReturns
from numpy import *
import ConfigParser

   
#Track the standard deviation of log returns for the product
class StdDev(DailyLogReturnsListener):

    instance = []

    def __init__(self,products,config_file):
        self._StdDev={}
        for product in products:
            self._StdDev[product] = empty(shape=(0))
        config = ConfigParser.ConfigParser()                                              #Get config file handler
        config.readfp(open(config_file,'r')) 
        self.periods = config.get('StdDev', 'periods').strip().split(",")
        self.periods = [int(i) for i in self.periods]                                     #Get periods to track Std as a list
        self.listeners=[]
        dailylogret = DailyLogReturns.GetUniqueInstance(products,config_file)
        dailylogret.AddListener(self)                                                     #Add as a listener to DailyLogReturns Indicator

    def AddListener(self,listener):
        self.listeners.append(listener)

    @staticmethod
    def GetUniqueInstance(products,config_file):
        if(len(StdDev.instance)==0):
            new_instance = StdDev(products,config_file)
            StdDev.instance.append(new_instance)
        return StdDev.instance[0]

    ##Update the standard deviation indicators on each ENDOFDAY event
    def OnDailyLogReturnsUpdate(self,product,DailyLogReturns):
        self._StdDev[product] = empty(shape=(0))
        for period in self.periods:
            n=DailyLogReturns.shape[0]
            if(n>=period):
                val = std(DailyLogReturns[n-period:n])
                self._StdDev[product]=append(self._StdDev[product],val)
            else:
                self._StdDev[product]=append(self._StdDev[product],nan)

