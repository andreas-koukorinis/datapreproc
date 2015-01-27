# cython: profile=True
#!/usr/bin/env python

import os
import sys
import shutil
from importlib import import_module
import ConfigParser
from dispatcher.dispatcher import Dispatcher
from utils.regular import get_trade_products, get_all_products, init_logs
from strategies.strategy_list import is_valid_strategy_name, get_module_name_from_strategy_name
from utils.global_variables import Globals
from utils.dbqueries import get_currency_and_conversion_factors

class Simulator:
    """ Performs the backtesting of the strategy
    
    Description: 
        Initialize the global variables in global_variables module
        Instantiate the strategy class and dispatcher class
        Call the dispatcher to start the simulation process
        Print the stats to the standard output

    Example run1: 
        python simulator.py test/IVWAS.cfg
    
    Example run2:
        python simulator.py test/IVWAS.cfg 2014-01-01 2014-10-31

    Args: config_file_path <start_date> <end_date>
        config_file_path: The relative path to the config file of the strategy
        start_date: The start_date of the simulation
        end_date: The end_date of the simulation
   
    Note:
        1) start_date and end_date are optional arguments, if not specified through command line then
           the ones mentioned in the config file will be used

    Output: Prints the performance stats to the standard output and log files in logs/config_name/ directory
    
    Returns: Nothing 
    """
    def __init__(self, _config_file, _start_date=None, _end_date=None):
        self._config = ConfigParser.ConfigParser()
        self._config.readfp( open( _config_file, 'r' ) )
        if _start_date is None:
            self._start_date = self._config.get('Dates', 'start_date')
        else:
            self._start_date = _start_date
        if _end_date is None:
            self._end_date = self._config.get('Dates', 'end_date')
        else:
            self._end_date = _end_date
    
        self._directory =  os.path.dirname(os.getcwd()) + "/logs/" + os.path.splitext(os.path.basename(_config_file))[0]+'/' # directory to store log files like positions,returns file
        if os.path.exists(self._directory):
            shutil.rmtree(self._directory)
        os.makedirs(self._directory)
        
        # Read product list from config file
        Globals.trade_products = get_trade_products(self._config)
        self._all_products = sorted(get_all_products(self._config))

        # Initialize the global variables
        Globals.conversion_factor, Globals.currency_factor, Globals.product_to_currency, Globals.product_type = get_currency_and_conversion_factors(self._all_products, self._start_date, self._end_date)

        # Initialize the log file handles
        self._log_dir =  os.path.expanduser('~') + "/logs/" + os.path.splitext(_config_file)[0].split('/')[-1] + '/'
        init_logs(self._config, self._log_dir, self._all_products)
       
        # Import the strategy class using 'Strategy'->'name' in config file
        self._stratfile = self._config.get ( 'Strategy', 'name' )  # Remove .py from filename
        if not(is_valid_strategy_name(self._stratfile)):
            print("Cannot proceed with invalid Strategy name")
            sys.exit()

        self.strategy_module_name = get_module_name_from_strategy_name(self._stratfile)
        self.TradeLogic = getattr(import_module('strategies.' + self.strategy_module_name), self._stratfile)  # Get the strategy class from the imported module

        # Instantiate the strategy
        # Strategy is written by the user and it inherits from TradeAlgorithm
        # TradeLogic here is the strategy class name converted to variable.Eg: UnleveredRP
        self._tradelogic_instance = self.TradeLogic(Globals.trade_products, self._all_products, self._start_date, self._end_date, self._config) # TODO Should take logfile as terminal arg

        # Instantiate the Dispatcher
        self._dispatcher = Dispatcher.get_unique_instance(self._all_products, self._start_date, self._end_date, self._config)

    def run(self):
        # Run the dispatcher to start the backtesting process
        self._dispatcher.run()
        print '\nTotal Tradable Days = %d'%(self._dispatcher.trading_days)
        # Call the performance tracker to display the stats
        self._tradelogic_instance.performance_tracker.show_results()
        Globals.reset()

if __name__ == '__main__':
    if len ( sys.argv ) < 2 :
        print "config_file <trading-startdate trading-enddate>"
        sys.exit(0)
    # Get handle of config file
    _config_file = sys.argv[1]
    if len ( sys.argv ) >= 4 :
        _start_date = sys.argv[2]
        _end_date = sys.argv[3]
        sim1 = Simulator(_config_file, _start_date, _end_date)
    else:
        sim1 = Simulator(_config_file)
    sim1.run()
