#!/usr/bin/env python
import os
import sys
import datetime
from importlib import import_module
import ConfigParser
from Dispatcher.Dispatcher import Dispatcher
from Utils.PrintIndicators import PrintIndicators
from Utils.Regular import get_all_products

def __main__() :
    # Command to run : python -W ignore GenData.py config_file <startdate enddate> <output_file>
    # Examples :
    # ./GenData.py test/config_PrintIndicators.cfg 2010-01-01 2013-12-31 ~/Downloads/temp.csv
    # ./GenData.py test/config_PrintIndicators.cfg # would take arguments from config file and create file in Data

    if len ( sys.argv ) < 2 :
        print "config_file <trading-startdate trading-enddate> <indicator_file>"
        sys.exit(0)
    # Get handle of config file
    _config_file = sys.argv[1]
    _config = ConfigParser.ConfigParser()
    _config.readfp( open( _config_file, 'r' ) )

    _directory = 'Data/'
    if not os.path.exists(_directory):
        os.makedirs(_directory)
    if ( len ( sys.argv ) >= 4 ) :
        _startdate = sys.argv[2]
        _enddate = sys.argv[3]
    else :
        _startdate=_config.get( 'Dates', 'start_date' )
        _enddate=_config.get( 'Dates', 'end_date' )

    if ( len ( sys.argv ) >= 5 ) :
        _indicator_file = sys.argv[4]
    else :
        _indicator_file = _directory+'print_indicators_'+os.path.splitext(_config_file)[0].split('/')[-1]+'.csv'

    # Read product list from config file
    _trade_products = _config.get( 'Products', 'trade_products' ).strip().split(",")

    _all_products = get_all_products( _config )


    PrintIndicators.get_unique_instance( _all_products, _startdate, _enddate, _indicator_file, _config )

    # Initialize Dispatcher using products list
    _dispatcher = Dispatcher.get_unique_instance( _all_products, _startdate, _enddate, _config )

    # Run the dispatcher to start the backtesting process
    _dispatcher.run()

    # Effective number of trading days will be less than [end_date-start_date] due to the warmup time specified by the user
    print '\nTotal Trading Days = %d'%( _dispatcher.trading_days )

__main__();
