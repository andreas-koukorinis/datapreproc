# -*- coding: utf-8 -*-
"""
Created on Tue Nov 18 10:51:21 2014

@author: Gurmeet Singh
"""

from numpy import *
from Indicator_Listeners import IndicatorListener
from DailyPrice import DailyPrice


# Track the standard deviation of log returns for the product
# In the config file this indicator will be specfied as : StdDev,product,period
class MovingAverage( IndicatorListener ):

    instances = {}

    def __init__( self, identifier, _startdate, _enddate, _config ):
        self.values = () # Tuple of the form (dt,value)
        self.identifier = identifier
        params = identifier.strip().split('.')
        self.product = params[1]
        self.period = float( params[2] )
        self.listeners = []
        daily_price = DailyPrice.get_unique_instance( 'DailyPrice.' + self.product, _startdate, _enddate, _config )
        daily_price.add_listener( self )

    def add_listener( self, listener ):
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config):
        if identifier not in MovingAverage.instances.keys() :
            new_instance = MovingAverage( identifier, _startdate, _enddate, _config )
            MovingAverage.instances[identifier] = new_instance
        return MovingAverage.instances[identifier]

    # Update moving average indicators on each ENDOFDAY event
    def on_indicator_update( self, identifier, daily_prices_dt ):
        daily_prices = array( [ item[1] for item in daily_prices_dt ] ).astype( float )
        n = daily_prices.shape[0]
        _start_index = max( 0, n - self.period )  # If sufficient lookback not available,use the available data only to compute indicator
        val = mean( daily_prices[ _start_index : n ] )
        if n < 2 or val == 0 :
            val=0.001  # Dummy value for insufficient lookback period(case where only 1 price)
        self.values = ( daily_prices_dt[-1][0], val )
        for listener in self.listeners: 
            listener.on_indicator_update( self.identifier, self.values )
