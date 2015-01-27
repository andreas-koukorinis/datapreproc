# cython: profile=True
'''Module to print the indicator values for all the indicators
specified in the config file
The module is used by gendata.py for research framework'''
import os
from importlib import import_module
from utils.regular import get_dt_from_date
from daily_indicators.indicator_listeners import IndicatorListener
from utils.global_variables import Globals

class PrintIndicators(IndicatorListener):

    def __init__(self, _startdate, _enddate, _indicators_file, _config):
        self.start_date = get_dt_from_date(_startdate).date()
        self.end_date = get_dt_from_date(_enddate).date()
        self.indicators_file = _indicators_file
        self.latest_indicator_values = {}
        self.all_indicator_values = {}
        # Read indicator list from config file
        self.identifiers = sorted(_config.get('daily_indicators', 'names').strip().split(" "))
        #Instantiate daily indicator objects
        for identifier in self.identifiers:
            indicator_name = identifier.strip().split('.')[0]
            module = import_module('daily_indicators.' + indicator_name)
            _indicator_class = getattr(module, indicator_name)
            _indicator_class.get_unique_instance(identifier, _startdate, _enddate, _config).add_listener(self)
            self.latest_indicator_values[identifier] = 0.0 # Default value for each indicator

    @staticmethod
    def get_unique_instance(_startdate, _enddate, _indicators_file, _config):
        if Globals.printindicators_instance is None:
            new_instance = PrintIndicators(_startdate, _enddate, _indicators_file, _config)
            Globals.printindicators_instance = new_instance
        return Globals.printindicators_instance

    def print_all_indicators(self):
        '''After the Simulation has been done, this function
           is called by gendata.py for printing the indicator
           values to csv file'''
        if not os.path.exists(os.path.dirname(self.indicators_file)):
            os.makedirs(os.path.dirname(self.indicators_file))
        _file = open(self.indicators_file, 'w')
        _header = 'date,' + ','.join(self.identifiers)
        _out = _header + '\n'
        _dates = sorted(self.all_indicator_values.keys())
        for _date in _dates:
            _line = [str(_date)]
            for _identifier in self.identifiers:
                _line.append(str(self.all_indicator_values[_date][_identifier]))
            _out = _out + ','.join(_line) + '\n'
        _file.write(_out)
        _file.close()

    def on_indicator_update(self, identifier, indicator_value):
        '''When an indicator is updated,update its latest value
           for printing later'''
        if type(indicator_value) is list:
            indicator_value = indicator_value[-1] # Some indicators return full history of values as list,others return only the most recent value
        current_date = indicator_value[0]
        self.latest_indicator_values[identifier] = indicator_value[1]
        if current_date >= self.start_date and current_date <= self.end_date:
            if current_date not in self.all_indicator_values.keys():
                self.all_indicator_values[current_date] = {}
                for _identifier in self.identifiers:
                    if _identifier.split('.')[0] == 'DailyLogReturns': # For dailylogreturns indicator use 0.0 as the default value
                        self.all_indicator_values[current_date][_identifier] = 0.0
                    else: # For other indicators use the latest value as the default value
                        self.all_indicator_values[current_date][_identifier] = self.latest_indicator_values[_identifier]
            self.all_indicator_values[current_date][identifier] = indicator_value[1]


