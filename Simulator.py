#!/usr/bin/env python

import os
import sys
from importlib import import_module
import ConfigParser
from Dispatcher.Dispatcher import Dispatcher
from Utils.Regular import get_all_products
from Strategies.strategy_list import is_valid_strategy_name, get_module_name_from_strategy_name
from Utils.global_variables import Globals
from Utils.DbQueries import get_currency_and_conversion_factors

def __main__() :
    """ Performs the backtesting of the strategy
    
    Description: 
        Initialize the global variables in global_variables module
        Instantiate the strategy class and dispatcher class
        Call the dispatcher to start the simulation process
        Print the stats to the standard output

    Example run1: 
        python Simulator.py test/IVWAS.cfg
    
    Example run2:
        python Simulator.py test/IVWAS.cfg 2014-01-01 2014-10-31

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
          
    if len ( sys.argv ) < 2 :
        print "config_file <trading-startdate trading-enddate>"
        sys.exit(0)
    # Get handle of config file
    _config_file = sys.argv[1]
    _directory = 'logs/'+os.path.splitext(os.path.basename(_config_file))[0]+'/' # directory to store log files like positions,returns file
    if not os.path.exists(_directory):
        os.makedirs(_directory)
    _config = ConfigParser.ConfigParser()
    _config.readfp( open( _config_file, 'r' ) )
    if len ( sys.argv ) >= 4 :
        _start_date = sys.argv[2]
        _end_date = sys.argv[3]
    else :
        _start_date = _config.get( 'Dates', 'start_date' )
        _end_date = _config.get( 'Dates', 'end_date' )

    # Read product list from config file
    _trade_products = _config.get( 'Products', 'trade_products' ).strip().split(",")
    _all_products = get_all_products( _config )

    # Initialize the global variables
    Globals.conversion_factor, Globals.currency_factor, Globals.product_to_currency = get_currency_and_conversion_factors(_all_products, _start_date, _end_date)

    # Import the strategy class using 'Strategy'->'name' in config file
    _stratfile = _config.get ( 'Strategy', 'name' )  # Remove .py from filename
    if not(is_valid_strategy_name(_stratfile)):
        print("Cannot proceed with invalid Strategy name")
        sys.exit()

    strategy_module_name = get_module_name_from_strategy_name(_stratfile)
    TradeLogic = getattr(import_module('Strategies.' + strategy_module_name), _stratfile)  # Get the strategy class from the imported module

    # Instantiate the strategy
    # Strategy is written by the user and it inherits from TradeAlgorithm
    # TradeLogic here is the strategy class name converted to variable.Eg: UnleveredRP
    _tradelogic_instance = TradeLogic( _trade_products, _all_products, _start_date, _end_date, _config , os.path.splitext(_config_file)[0].split('/')[-1] ) # TODO Should take logfile as terminal arg

    # Instantiate the Dispatcher
    _dispatcher = Dispatcher.get_unique_instance( _all_products, _start_date, _end_date, _config )

    # Run the dispatcher to start the backtesting process
    _dispatcher.run()

    print '\nTotal Tradable Days = %d'%( _dispatcher.trading_days )

    # Call the performance tracker to display the stats
    _tradelogic_instance.performance_tracker.show_results()

if __name__ == '__main__':
    __main__()
