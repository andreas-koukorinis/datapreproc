import sys
import ast
import datetime
from importlib import import_module
import ConfigParser
from OrderManager.OrderManager import OrderManager
from Dispatcher.Dispatcher import Dispatcher
from BookBuilder.BookBuilder import BookBuilder
from Utils.PrintIndicators import PrintIndicators

def __main__() :
    # Command to run : python -W ignore Simulator.py config_file
    # Example        : python -W ignore Simulator.py config.txt

    if len ( sys.argv ) < 2 :
        print "arguments <trading-startdate trading-enddate>"
        sys.exit(0)
    # Get handle of config file
    config_file = sys.argv[1]
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file,'r'))

    # Read product list from config file
    products = config.get( 'Products', 'symbols' ).strip().split(",")
    # If there is fES1 ... make sure fES2 is also there, if not add it

    PrintIndicators.get_unique_instance( products, config_file )

    # Initialize Dispatcher using products list
    _dispatcher = Dispatcher.get_unique_instance( products, config_file )

    # Run the dispatcher to start the backtesting process
    _dispatcher.run()

    # Effective number of trading days will be less than [end_date-start_date] due to the warmup time specified by the user
    print '\nTotal Trading Days = %d'%( _dispatcher.trading_days )

__main__();
