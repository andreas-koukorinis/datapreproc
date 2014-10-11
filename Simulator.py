import sys,ast,datetime
import ConfigParser
from Performance.PerformanceTracker import PerformanceTracker
from OrderManager.OrderManager import OrderManager
from Dispatcher.Dispatcher import Dispatcher
from BackTester.BackTester import BackTester
from BookBuilder.BookBuilder import BookBuilder
from Utils.Regular import getdtfromdate
from Utils.DbQueries import conv_factor
from importlib import import_module
from Portfolio import Portfolio

def __main__() :
    #Command to run : python -W ignore Simulator.py config_file
    #Example        : python -W ignore Simulator.py config.txt

    if len ( sys.argv ) < 2 :
        print "arguments <trading-startdate trading-enddate>"
        sys.exit(0)
    #Get handle of config file
    config_file = sys.argv[1]
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file,'r'))

    # Read product list from config file
    products = config.get('Products', 'symbols').strip().split(",")
    # if there is fES1 ... make sure fES2 is also there, if not add it

    # TODO { gchak } : we only want performance_tracker for the products we are trading and not all the ones we have data for.
    # TODO { gchak } : we probably hsould align the performance_tracker object to one instance of the TradeLogic
    # Initialize performance tracker with list of products
    performance_tracker = PerformanceTracker.GetUniqueInstance ( products, config_file )

    #Import the strategy class using 'Strategy'->'name' in config file
    stratfile = config.get('Strategy', 'name') #Remove .py from filename
    module = import_module('Strategies.'+stratfile) #Import the module corresponding to the filename
    TradeLogic = getattr(module,stratfile) #Get the strategy class from the imported module

    # Initialize the strategy
    # Strategy is written by the user and it inherits from TradeAlgorithm,
    # TradeLogic here is the strategy class name converted to variable.Eg: UnleveredRP
    # Doubt { gchak } : I think this is one class where we don't need GetUniqueInstance. For instance in
    #     DVC::basetrade we have a for-loop over the stratfile that creates a strategy insatnce for each
    #     line in the stratfile. This is used for optimization. But even without optimization, we probably
    #     don't foresee any other class creating a strategy instance. Hence this GetUniqueInstance will only
    #     be called once.
    strategy = TradeLogic.GetUniqueInstance(products,config_file,TradeLogic)

    # Give strategy the access to the portfolio instance,
    # so that it can calculate its current worth and decide positions based on it.
    strategy.portfolio = Portfolio.GetUniqueInstance(products,config_file)

    #Initialize Dispatcher using products list
    dispatcher = Dispatcher.GetUniqueInstance(products,config_file)

    #Run the dispatcher to start the backtesting process
    dispatcher.run()

    #Effective number of trading days will be less than [end_date-start_date] due to the warmup time specified by the user
    print '\nTotal Trading Days = %d'%(dispatcher.trading_days)

    #Call the performance tracker to display the results and plot the graph of cumulative PnL
    performance_tracker.showResults()

__main__();
