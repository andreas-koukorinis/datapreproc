import math
from indicator_listeners import IndicatorListener
from daily_log_returns import DailyLogReturns


class StdDev( IndicatorListener ):
    """Track the standard deviation of log returns for the product

       Identifier: StdDev.product_name.period
                   Eg: StdDev.fES.21

       Listeners: Other indicators like AverageStdDev

       Listening to: DailyLogReturns of the corresponding product
    """
    instances = {}

    def __init__( self, identifier, _startdate, _enddate, _config ):
        """Initializes the required variables like identifier, period, current_sum, current_num, current_pow_sum etc
           and starts listening to dailylogreturns of the corresponding product
           
           Args:
               _identifier(string): The identifier for this indicator. Eg: StdDev.fES.21
               _startdate(date object): The start date of the simulation
               _enddate(date object): The end date of the simulation
               _config(ConfigParser handle): The handle to the config file of the strategy            
        """        
        self.values = (0.0,1) # Tuple of the form (dt,value) #TODO better init for default dt
        self.identifier = identifier
        params = identifier.strip().split('.')
        self.product = params[1]
        self.period = int( params[2] )
        # To maintain running stdDev
        self.current_sum = 0.0 # Sum of log returns in the window
        self.current_num = 0.0 # Number of elements in the window
        self.current_pow_sum = 0.0 # Sum of squares of log returns in the window
        self.listeners = []
        daily_log_ret = DailyLogReturns.get_unique_instance( 'DailyLogReturns.' + self.product, _startdate, _enddate, _config )
        daily_log_ret.add_listener( self )

    def get_stdev(self):
        if len(self.values) >= 2:
            return (self.values[1])
        else:
            return 1
    
    def add_listener( self, listener ):
        """Used by other classes to register as on_indicator_update listener of the dispatcher

           Args:
               listener(object): The object of the class which wants to listen

           Returns: Nothing       
        """
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config):
        """This static function is used by other classes to add themselves as a listener to the StdDev"""
        if identifier not in StdDev.instances.keys() :
            new_instance = StdDev( identifier, _startdate, _enddate, _config )
            StdDev.instances[identifier] = new_instance
        return StdDev.instances[identifier]

    # Update the standard deviation indicators on each ENDOFDAY event
    # Efficient version
    def on_indicator_update( self, identifier, daily_log_returns_dt ):
        """On a logreturn update, this function is called by the DailyLogReturns instance and the new stddev is computed here
           On computation of a new stddev value, the tuple (date, new_stddev_value) is passed onto the listeners of this indicator

           Args:
               identifier(string): The identifier for the indicator whose update has come.In this case DailyLogReturns.product 
               daily_log_returns_dt(list of tuples (date, logretvalue)): THe logreturns of the product

           Note:
               Here stddev is computed in an online fashion for efficiency purposes and hence the variables current_sum, current_num, current_pow_sum

           Returns: Nothing
        """ 
        n = len(daily_log_returns_dt)
        if n > self.period:
            self.current_sum =  self.current_sum - daily_log_returns_dt[n-self.period-1][1] + daily_log_returns_dt[n-1][1] 
            self.current_pow_sum =  self.current_pow_sum - daily_log_returns_dt[n-self.period-1][1]*daily_log_returns_dt[n-self.period-1][1] + daily_log_returns_dt[n-1][1]*daily_log_returns_dt[n-1][1]
            val = math.sqrt(self.current_pow_sum/self.current_num - (self.current_sum/self.current_num)*(self.current_sum/self.current_num))
        elif n < 2:
            val = 0.001 # Dummy value for insufficient lookback period(case where only 1 log return)
            if n == 1:
                self.current_sum = daily_log_returns_dt[n-1][1]
                self.current_pow_sum = daily_log_returns_dt[n-1][1]*daily_log_returns_dt[n-1][1]
                self.current_num = 1
        else:
            self.current_sum = self.current_sum + daily_log_returns_dt[n-1][1]
            self.current_pow_sum =  self.current_pow_sum + daily_log_returns_dt[n-1][1]*daily_log_returns_dt[n-1][1]
            self.current_num += 1
            val = sqrt(self.current_pow_sum/self.current_num - (self.current_sum/self.current_num)*(self.current_sum/self.current_num))
        if math.isnan(val):
            print ("something wrong")
        self.values = (daily_log_returns_dt[-1][0], val)
        for listener in self.listeners: 
            listener.on_indicator_update(self.identifier, self.values)
