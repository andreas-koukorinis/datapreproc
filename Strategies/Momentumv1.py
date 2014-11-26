import sys
from numpy import *
from importlib import import_module
from Algorithm.TradeAlgorithm import TradeAlgorithm
from Utils.Regular import check_eod
from DailyIndicators.Indicator_List import is_valid_daily_indicator

class Momentumv1(TradeAlgorithm):

    def init( self, _config ):
        self.day=-1
        self.rebalance_frequency = _config.getint( 'Parameters', 'rebalance_frequency' )
        self.periods = _config.get('Strategy','periods').split(',')
        for product in self.products:
            for period in self.periods:
                indicator_name = 'MovingAverage'
                indicator = indicator_name + '.' + product + '.' + period
                if is_valid_daily_indicator(indicator_name):
                    module = import_module('DailyIndicators.' + indicator_name)
                    Indicatorclass = getattr( module, indicator_name )
                    self.daily_indicators[indicator] = Indicatorclass.get_unique_instance(indicator, self.start_date, self.end_date, _config)

    def on_events_update(self,events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number
           
        # If today is the rebalancing day,then use indicators to calculate new positions to take
        if all_eod and self.day % self.rebalance_frequency == 0 :
            # Calculate weights to assign to each product using indicators
            weights = {}
            sum_weights = 0.0
            for product in self.products:
                weights[product] = sign(self.daily_indicators['MovingAverage.' + product + '.' + self.periods[0] ].values[1]-self.daily_indicators['MovingAverage.' + product + '.' + self.periods[1]].values[1])
                sum_weights = sum_weights + abs(weights[product]) # Here abs does not make any difference,but in general should do
            for product in self.products:
                weights[product] = weights[product]/sum_weights                
            self.update_positions( events[0]['dt'], weights )
        else:
            self.rollover( events[0]['dt'] )
