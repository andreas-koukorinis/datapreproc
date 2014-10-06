import sys,ast,datetime
from Portfolio import Portfolio
from PerformanceTracker import PerformanceTracker
from OrderManager import OrderManager
from TradeLogic import TradeLogic
from Dispatcher import Dispatcher
from BackTester import BackTester
from BookBuilder import BookBuilder
from Utils import getdtfromdate,conversion_factor

#Command to run : python Simulator.py start_date end_date list_products initial_capital
#Example        : python Simulator.py 2014-01-01 2014-08-20 "['fES1','fTY1']" 1000000

#Take arguments from terminal
start_date = sys.argv[1]
end_date = sys.argv[2]
products = ast.literal_eval(sys.argv[3])
initial_capital = float(sys.argv[4])
conv_factor = conversion_factor(products)

#Initialize portfolio using initial_capital and list of products
portfolio = Portfolio(initial_capital,products)

#Initialize performance tracker with initial_capital,list of products
performance_tracker = PerformanceTracker(initial_capital,products,conv_factor)   

#Initialize Bacltesting objects for each product 
#Pass instance of Portfolio and Performance Tracker to each Backtester since Backtester updates the portfolio and passes the control to Performance Tracker
bt_objects = {}
for product in products:
    bt_objects[product] = BackTester(product,portfolio,performance_tracker,conv_factor[product])

#Initialize the Order Manager
#Pass all the backtester instances to the Order Manager because Order Manager will send the orders to the Backtester  
order_manager = OrderManager(bt_objects)

#Initialize the strategy,which is an instance of TradeLogic
#TradeLogic is written by the user and it inherits from TradeAlgorithm
#Pass OrderManager to the strategy as the strategy will send orders to the order manager
#Pass Portfolio to the strategy since the strategy can make decisions based on its current portfolio
#Pass Performance tracker to the strategy since strategy can make decisions based on its past performance
strategy = TradeLogic(order_manager,portfolio,performance_tracker,products,conv_factor)

#Initialize the Book Builder objects 
#Pass strategy to the book builders since book builders will update the indicators which are part of the strategy
#Pass backtester for the given product to its book builder as book builder will call the backtester on every book update 
bb_objects = {}
for product in products:
    bb_objects[product] = BookBuilder(product,strategy,bt_objects[product],strategy.maxentries_dailybook,strategy.maxentries_intradaybook)

#Allow performance tracker to access the book builder instances to evaluate the worth of the portfolio at a particular instance of time
performance_tracker.bb_objects = bb_objects

#Store effective startdate(so that we can track daily performance)
performance_tracker.date = (getdtfromdate(start_date)+datetime.timedelta(days=strategy.warmupdays)).date()

#Initialize Dispatcher
#Pass all BookBuilder instances to the Dispatcher since Dispatcher will pass control to the book builders on an event
#Pass strategy to the Dispatcher since the dispatcher will pass control to the strategy for events on each timestamp after all books for that timestamp have been updated
#Pass warmupdays so that OnEventListener function of the strategy is not called in the initial 'warmupdays' number of days,only books are updated in these days
dispatcher = Dispatcher(start_date,end_date,products,bb_objects,strategy,strategy.warmupdays)

#Run the dispatcher to start the backtesting process
dispatcher.run()

#Effective number of trading days will be less than [end_date-start_date] due to the warmup time specified by the user
print 'Total Trading Days = %d\n'%(dispatcher.days-dispatcher.warmupdays)

#Call the performance tracker to display the results and plot the graph of cumulative returns
performance_tracker.showResults()
