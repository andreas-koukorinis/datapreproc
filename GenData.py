#!/usr/bin/env python
import os
import sys
import ast
import datetime
from importlib import import_module
import ConfigParser
from Dispatcher.Dispatcher import Dispatcher
from BookBuilder.BookBuilder import BookBuilder
from Utils.PrintIndicators import PrintIndicators

def __main__() :
    # Command to run : python -W ignore GenData.py config_file <startdate enddate> <output_file>
    # Examples :
    # ./GenData.py test/config_PrintIndicators.cfg 2010-01-01 2013-12-31 ~/Downloads/temp.csv
    # ./GenData.py test/config_PrintIndicators.cfg # would take arguments from config file and create file in Data

    if len ( sys.argv ) < 2 :
        print "config_file <trading-startdate trading-enddate> <indicator_file>"
        sys.exit(0)
    # Get handle of config file
    config_file = sys.argv[1]
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file,'r'))
    directory = 'Data/'
    if ( len ( sys.argv ) >= 4 ) :
        _startdate = sys.argv[2]
        _enddate = sys.argv[3]
    else :
        _startdate=config.get( 'Dates', 'start_date' )
        _enddate=config.get( 'Dates', 'end_date' )

    if ( len ( sys.argv ) >= 5 ) :
        _indicator_file = sys.argv[4]
    else :
        _indicator_file = directory+'print_indicators_'+os.path.splitext(config_file)[0].split('/')[-1]+'.csv'

    # Read product list from config file
    products = config.get( 'Products', 'symbols' ).strip().split(",")
    # If there is fES1 ... make sure fES2 is also there, if not add it

    PrintIndicators.get_unique_instance( products, _startdate, _enddate, _indicator_file, config_file )

    # Initialize Dispatcher using products list
    _dispatcher = Dispatcher.get_unique_instance( products, _startdate, _enddate, config_file )

    # Run the dispatcher to start the backtesting process
    _dispatcher.run()

    # Effective number of trading days will be less than [end_date-start_date] due to the warmup time specified by the user
    print '\nTotal Trading Days = %d'%( _dispatcher.trading_days )

__main__();
