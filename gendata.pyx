'''Script to generate indicator values for research framework'''
#!/usr/bin/env python
import os
import sys
import ConfigParser
from dispatcher.dispatcher import Dispatcher
from utils.dbqueries import get_currency_and_conversion_factors
from utils.print_indicators import PrintIndicators
from utils.regular import get_all_products, get_trade_products
from utils.global_variables import Globals

class Gendata:
    # Command to run : python -W ignore gendata.py config_file <startdate enddate> <output_file>
    # Examples :
    # ./gendata.py test/config_PrintIndicators.cfg 2010-01-01 2013-12-31 ~/Downloads/temp.csv
    # ./gendata.py test/config_PrintIndicators.cfg # would take arguments from config file and create file in Data

    def __init__(self, _config_file, _start_date=None, _end_date=None, _indicator_file=None):
        _config = ConfigParser.ConfigParser()
        _config.readfp( open( _config_file, 'r' ) )
        _directory = os.path.expanduser('~') + '/logs/data/'

        if _start_date is None:
            _start_date = _config.get('Dates', 'start_date')
        if _end_date is None:
            _end_date = _config.get('Dates', 'end_date')
        if _indicator_file is None:
            _indicator_file = _directory+'indicators_'+os.path.splitext(_config_file)[0].split('/')[-1]+'.csv'

        if not os.path.exists(_directory):
            os.makedirs(_directory)
        Globals.trade_products = get_trade_products(_config)
        Globals.all_products = get_all_products(Globals.trade_products)
        Globals.conversion_factor, Globals.currency_factor, Globals.product_to_currency, Globals.product_type = get_currency_and_conversion_factors(Globals.all_products, _start_date, _end_date)
        self._print_indicators_instance = PrintIndicators.get_unique_instance(_start_date, _end_date, _indicator_file, _config)

        # Initialize Dispatcher using products list
        self._dispatcher = Dispatcher.get_unique_instance(Globals.all_products, _start_date, _end_date, _config)

    def run(self):
        # Run the dispatcher to start the backtesting process
        self._dispatcher.run()
        print '\nTotal Tradable Days = %d'%(self._dispatcher.trading_days)
        self._print_indicators_instance.print_all_indicators()
        Globals.reset()

if __name__ == '__main__':
    if len ( sys.argv ) < 2 :
        print "config_file <trading-startdate trading-enddate indicator_file>"
        sys.exit(0)

    _config_file = sys.argv[1]
    if len ( sys.argv ) >= 4 :
        _start_date = sys.argv[2]
        _end_date = sys.argv[3]
    else:
        _start_date = None
        _end_date = None

    _directory = os.path.expanduser('~') + 'data/'
    if len(sys.argv) >= 5:
        _indicator_file = sys.argv[4]
    else:
        _indicator_file = None
    sim = Gendata(_config_file, _start_date, _end_date, _indicator_file)
    sim.run()
