#!/usr/bin/env python

import os
import sys
from importlib import import_module
import ConfigParser
from Dispatcher.Dispatcher import Dispatcher
from Utils.Regular import get_all_products

def __main__() :
    if len ( sys.argv ) < 2 :
        print "config_file <trading-startdate trading-enddate>"
        sys.exit(0)
    # Get handle of config file
    _config_file = sys.argv[1]
    _config = ConfigParser.ConfigParser()
    _config.readfp( open( _config_file, 'r' ) )
    if len ( sys.argv ) >= 4 :
        _start_date = sys.argv[2]
        _end_date = sys.argv[3]
    else :
        _start_date = _config.get( 'Dates', 'start_date' )
        _end_date = _confi_all_productsg.get( 'Dates', 'end_date' )

    # Read product list from config file
    _trade_products = _config.get( 'Products', 'trade_products' ).strip().split(",")

    _all_products = get_all_products( _config )

    # Import the strategy class using 'Strategy'->'name' in config file
    _stratfile = _config.get ( 'Strategy', 'name' )  # Remove .py from filename
    TradeLogic = getattr ( import_module ( 'Strategies.' + _stratfile ) , _stratfile )  # Get the strategy class from the imported module

    # Initialize the strategy
    # Strategy is written by the user and it inherits from TradeAlgorithm,
    # TradeLogic here is the strategy class name converted to variable.Eg: UnleveredRP
    _tradelogic_instance = TradeLogic( _trade_products, _all_products, _start_date, _end_date, _config , os.path.splitext(_config_file)[0].split('/')[-1] ) # TODO Should take logfile as terminal arg

    # Initialize Dispatcher using products list
    _dispatcher = Dispatcher.get_unique_instance( _all_products, _start_date, _end_date, _config )

    # Run the dispatcher to start the backtesting process
    _dispatcher.run()

    # Effective number of trading days will be less than [end_date-start_date] due to the warmup time specified by the user
    print '\nTotal Trading Days = %d'%( _dispatcher.trading_days )

    # Call the performance tracker to display the results and plot the graph of cumulative PnL
    _tradelogic_instance.performance_tracker.show_results()

__main__();
