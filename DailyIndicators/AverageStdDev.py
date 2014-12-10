from numpy import *
from Indicator_Listeners import IndicatorListener
from StdDev import StdDev

class AverageStdDev( IndicatorListener ):
    """Track the average standard deviation of log returns over a list of periods for the product.
    In the config file this indicator will be specfied as : StdDev,product,period

    """
    instances = {}

    def __init__( self, identifier, _startdate, _enddate, _config ):
        self.values = () # Tuple of the form (dt,value)
        self.identifier = identifier
        params = identifier.strip().split('.')
        self.product = params[1]
        self.periods = []
        for i in range(2,len(params)):
            self.periods.append(int( params[i] ) )
            _identifier = 'StdDev.' + self.product + '.' + str(self.periods[i-2])
            StdDev.get_unique_instance( _identifier, _startdate, _enddate, _config ).add_listener( self )
        self.num_listening = len(self.periods)
        self.num_updates = 0
        self.current_sum = 0.0
        self.listeners = []

    def add_listener( self, listener ):
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config):
        if identifier not in AverageStdDev.instances.keys() :
            new_instance = AverageStdDev( identifier, _startdate, _enddate, _config )
            AverageStdDev.instances[identifier] = new_instance
        return AverageStdDev.instances[identifier]

    # Update the standard deviation indicators on each ENDOFDAY event
    # Efficient version
    def on_indicator_update( self, identifier, values ):
        self.num_updates += 1 # Received another update
        self.current_sum += values[1] # Update the current sum of stdevs
        if self.num_updates == self.num_listening: # If we have received all updates
            val = self.current_sum/float(self.num_listening) # Compute the average of stdevs
            self.num_updates = 0 # Set num updates back to 0
            self.current_sum = 0.0 # Set current sum back to 0
            self.values = ( values[0], val )
            for listener in self.listeners: 
                listener.on_indicator_update( self.identifier, self.values )
