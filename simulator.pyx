# cython: profile=True
#!/usr/bin/env python

import os
import sys
import shutil
import argparse
from importlib import import_module
import ConfigParser
from dispatcher.dispatcher import Dispatcher
from utils.regular import get_all_trade_products, get_all_products, init_logs
from strategies.strategy_list import is_valid_strategy_name, get_module_name_from_strategy_name
from utils.global_variables import Globals
from utils.dbqueries import get_currency_and_conversion_factors
from utils.json_parser import JsonParser

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
    def __init__(self, args):
        self.log_dir = args.logs_output_path
        self.to_store = args.store
        self.config_file = args.config_file
        if os.path.exists(self.log_dir):
            shutil.rmtree(self.log_dir)
        os.makedirs(self.log_dir)
        if os.path.splitext(self.config_file)[1] == '.json':
            _output_cfg_dir = self.log_dir
            JsonParser().json_to_cfg(self.config_file, _output_cfg_dir)
            self.config_file = _output_cfg_dir + 'agg.cfg'    
        self.config = ConfigParser.ConfigParser()
        self.config.readfp( open( self.config_file, 'r' ) )

        if args.sim_start_date is None:
            self.start_date = self.config.get('Dates', 'start_date')
        else:
            self.start_date = args.sim_start_date
        if args.sim_end_date is None:
            self.end_date = self.config.get('Dates', 'end_date')
        else:
            self.end_date = args.sim_end_date
        self.json_output_path = args.json_output_path
    
        # Read product list from config file
        Globals.trade_products = get_all_trade_products(self.config)
        Globals.all_products = sorted(get_all_products(Globals.trade_products))
        Globals.config_file = self.config_file
        # Initialize the global variables
        Globals.conversion_factor, Globals.currency_factor, Globals.product_to_currency, Globals.product_type = get_currency_and_conversion_factors(Globals.all_products, self.start_date, self.end_date)

        # Initialize the log file handles
        init_logs(self.config, self.log_dir, Globals.all_products)
       
        # Import the strategy class using 'Strategy'->'name' in config file
        self.stratfile = self.config.get ( 'Strategy', 'name' )  # Remove .py from filename
        if not(is_valid_strategy_name(self.stratfile)):
            sys.exit('Cannot proceed with invalid Strategy name')

        self.strategy_module_name = get_module_name_from_strategy_name(self.stratfile)
        self.TradeLogic = getattr(import_module('strategies.' + self.strategy_module_name), self.stratfile)  # Get the strategy class from the imported module

        # Instantiate the strategy
        # Strategy is written by the user and it inherits from TradeAlgorithm
        # TradeLogic here is the strategy class name converted to variable.Eg: UnleveredRP
        self.tradelogic_instance = self.TradeLogic(Globals.trade_products, Globals.all_products, self.start_date, self.end_date, self.config) # TODO Should take logfile as terminal arg

        # Instantiate the Dispatcher
        self.dispatcher = Dispatcher.get_unique_instance(Globals.all_products, self.start_date, self.end_date, self.config)

    def run(self):
        # Run the dispatcher to start the backtesting process
        self.dispatcher.run()
        print '\nTotal Tradable Days = %d'%(self.dispatcher.trading_days)
        # Call the performance tracker to display the stats
        (dates, log_returns, leverage, stats) = self.tradelogic_instance.performance_tracker.show_results()
        #if sim_json is not None:
        #    with open(self.json_output_path,'w') as f:
        #        f.write(sim_json)
        if self.to_store == 1:
            JsonParser().dump_sim_to_db(Globals.config_file, dates, log_returns, leverage, stats)
        Globals.reset()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    parser.add_argument('-sd', type=str, help='Sim Start date\nEg: -sd 2014-06-01\n Default is config_start_date',default=None, dest='sim_start_date')
    parser.add_argument('-ed', type=str, help='Sim End date\nEg: -ed 2014-10-31\n Default is config end_date',default=None, dest='sim_end_date')
    parser.add_argument('-o', type=str, help='Json Output path\nEg: -o ~/logs/file.json\n Default is in log dir',default=None, dest='json_output_path')
    parser.add_argument('-logs', type=str, help='Logs Output path\nEg: -logs ~/logs/\n Default is in log dir',default=None, dest='logs_output_path')
    args = parser.parse_args()
    if args.logs_output_path is None:
        args.logs_output_path = '~/logs/' + os.path.splitext(args.config_file)[0].split('/')[-1] + '/'
    args.logs_output_path.replace('~', os.path.expanduser('~'))
    if args.json_output_path is None:
        args.json_output_path = args.logs_output_path + 'output.json'
    sim = Simulator(args)
    sim.run()
