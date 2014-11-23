import os
import datetime
from numpy import *
import scipy.stats as ss
import datetime
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pylab
import pickle

from BackTester.BackTester_Listeners import BackTesterListener
from BackTester.BackTester import BackTester
from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import EndOfDayListener
from Utils.Regular import check_eod,get_dt_from_date,get_next_futures_contract,is_future
from Utils.DbQueries import conv_factor
from BookBuilder.BookBuilder import BookBuilder

# TODO {gchak} PerformanceTracker is probably a class that just pertains to the performance of one strategy
# We need to change it from listening to executions from BackTeser, to being called on from the OrderManager,
# which will in turn listen to executions from the BackTester

'''Performance tracker listens to the Dispatcher for concurrent events so that it can record the daily returns
 It also listens to the Backtester for any new filled orders
 At the end it shows the results and plot the cumulative PnL graph
 It outputs the list of [dates,dailyreturns] to returns_file for later analysis
 It outputs the portfolio snapshots and orders in the positions_file for debugging
'''
class PerformanceTracker( BackTesterListener, EndOfDayListener ):

    def __init__( self, products, _startdate, _enddate, _config, _log_filename ):
        self.date = get_dt_from_date(_startdate).date()  #The earliest date for which daily stats still need to be computed
        self.positions_file = 'logs/'+_log_filename+'/positions.txt'
        self.returns_file = 'logs/'+_log_filename+'/returns.txt'
        self.pnl_graph_file = 'logs/'+_log_filename+'/pnl_graph.png'
        self.products = products
        self.conversion_factor = conv_factor(products)
        self.num_shares_traded = dict([(product,0) for product in self.products])
        self.dates = []
        self.PnL = 0
        self.net_returns = 0
        self.initial_capital = _config.getfloat( 'Parameters', 'initial_capital' )
        self.value = array( [ self.initial_capital ] )  # Track end of day values of the portfolio
        self.PnLvector = empty(shape=(0))
        self.annualized_PnL = 0
        self.annualized_stdev_PnL = 0
        self._annualized_returns_percent = 0
        self.annualized_stddev_returns = 0
        self.sharpe = 0
        self.daily_returns = empty ( shape=(0) )
        self.daily_log_returns = empty ( shape=(0) )
        self._monthly_nominal_returns_percent = empty ( shape=(0) )
        self._quarterly_nominal_returns_percent = empty ( shape=(0) )
        self._yearly_nominal_returns_percent = empty ( shape=(0) )
        self.dml = 0
        self.mml = 0
        self._worst_10pc_quarterly_returns = 0
        self._worst_10pc_yearly_returns = 0
        self.current_loss = 0
        self.current_drawdown = 0
        self.max_drawdown_percent = 0
        self.max_drawdown_dollar = 0
        self.return_by_maxdrawdown = 0
        self._annualized_pnl_by_max_drawdown_dollar = 0
        self.skewness = 0
        self.kurtosis = 0
        self.trading_cost = 0
        self.portfolio_snapshots = []
        self.total_orders = 0

        # Listens to end of day combined event to be able to compute the market ovement based effect on PNL
        dispatcher = Dispatcher.get_unique_instance( products, _startdate, _enddate, _config )
        dispatcher.add_end_of_day_listener( self )

        self.bb_objects={}
        for product in products:
            backtester = BackTester.get_unique_instance( product, _startdate, _enddate, _config ) 
            backtester.add_listener( self ) # Listen to Backtester for filled orders
            self.bb_objects[product] = BookBuilder.get_unique_instance( product, _startdate, _enddate, _config )

    def on_order_update( self, filled_orders, current_date ):
        for order in filled_orders:
            self.num_shares_traded[order['product']] = self.num_shares_traded[order['product']] + abs(order['amount'])
            self.trading_cost = self.trading_cost + order['cost']
            self.total_orders = self.total_orders + 1
        self.print_filled_orders( filled_orders )

    # Called by Dispatcher
    def on_end_of_day( self, date ):
        self.compute_daily_stats( date )
        _current_dd_log = self.current_dd( self.daily_log_returns )
        self.current_drawdown = abs( ( exp( _current_dd_log ) - 1 ) * 100 )
        self.current_loss = self.initial_capital - self.value[-1]
        self.print_snapshot( date )

    #Find the latest price prior to 'date'
    def find_most_recent_price( self, book, date ):
        if len(book) < 1 :
            sys.exit('ERROR: warmupdays not sufficient')
        elif book[-1][0].date() <= date :
            return book[-1][1]
        else:
            return self.find_most_recent_price( book[:-1], date )

    #Find the latest price prior to 'date' for futures product
    def find_most_recent_price_future( self, book1, book2, date ):
        if len(book1) < 1 :
            sys.exit('ERROR: warmupdays not sufficient')
        elif book1[-1][0].date() <= date and book1[-1][2] : #If the day was settlement day,then use second futures contract price
            return book2[-1][1]
        elif book1[-1][0].date() <= date and not book1[-1][2] : #If the day was not settlement day,then use first futures contract price
            return book1[-1][1]
        else:
            return self.find_most_recent_price_future( book1[:-1], book2[:-1], date )

    # Computes the portfolio value at ENDOFDAY on 'date'
    def get_portfolio_value(self,date):
        #self.print_prices(self.date) # TODO Only if needed. For instance in zero-logging mode or optimization mode, we will want to disable this.
        netValue = self.portfolio.cash
        for product in self.products:
            if self.portfolio.num_shares[product] != 0:
                if is_future( product ):
                    current_price = self.find_most_recent_price_future( self.bb_objects[product].dailybook, self.bb_objects[ get_next_futures_contract(product) ].dailybook, date )
                else:
                    current_price = self.find_most_recent_price( self.bb_objects[product].dailybook, date )
                netValue = netValue + current_price * self.portfolio.num_shares[product] * self.conversion_factor[product]
        return netValue

    def print_prices( self, date ):
        s = ''
        for product in self.products:
            if self.num_shares_traded[product] != 0:
                if is_future( product ):
                    current_price = self.find_most_recent_price_future( self.bb_objects[product].dailybook, self.bb_objects[ get_next_futures_contract(product) ].dailybook, date )
                else:
                    current_price = self.find_most_recent_price( self.bb_objects[product].dailybook, date )
                s = s + product + ' ' + str( current_price )
        text_file = open( self.positions_file, "a" ) # TODO {gchak} consider opening file once, and sharing the same file pointer among all the PerformanceTracker objects
        text_file.write("Prices at end of day %s are: %s\n" % (date,s))
        text_file.close()

    # Computes the daily stats for the most recent trading day prior to 'date'
    # TOASK {gchak} Do we ever expect to run this function without current date ?
    def compute_daily_stats(self,date):
        self.date=date
        if(self.total_orders>0): #If no orders have been filled,it implies trading has not started yet       
            todaysValue = self.get_portfolio_value(self.date)
            #print date,todaysValue
            self.value = append ( self.value, todaysValue )
            self.PnLvector = append ( self.PnLvector, ( self.value[-1]-self.value[-2] ) )  # daily PnL = Value of portfolio on last day - Value of portfolio on 2nd last day
            if ( self.value[-1] <= 0 ) :
                _logret_today = -1000; # real value is -inf
            else :
                _logret_today = log ( self.value[-1] / self.value[-2] )
            self.daily_log_returns = append ( self.daily_log_returns, _logret_today )

            self.dates.append(self.date)

    def print_filled_orders(self,filled_orders):
        if(len(filled_orders)==0): return
        s = 'ORDER FILLED : '
        for order in filled_orders:
            s = s + 'id: %d  product: %s  amount: %f  cost: %f  value: %f  fill_price: %f'%(order['id'],order['product'],order['amount'],order['cost'],order['value'],order['fill_price'])
        text_file = open(self.positions_file, "a")
        text_file.write("%s\n" % s)
        text_file.close()

    def print_snapshot(self,date):
        text_file = open(self.positions_file, "a")
        if(self.PnLvector.shape[0]>0):
            s = ("\nPortfolio snapshot at EndOfDay %s\nCash:%f\tPositions:%s Portfolio Value:%f PnL for today: %f \n\n" % (date,self.portfolio.cash,str(self.portfolio.num_shares),self.value[-1],self.PnLvector[-1]))
        else:
            s = ("\nPortfolio snapshot at EndOfDay %s\nCash:%f\tPositions:%s Portfolio Value:%f Trading has not yet started\n\n" % (date,self.portfolio.cash,str(self.portfolio.num_shares),self.value[-1]))
        text_file.write(s)
        text_file.close()

    # Calculates the current drawdown i.e. the maximum drawdown with end point as the latest return value 
    def current_dd( self, returns ):
        if returns.shape[0] < 2:
            return 0.0
        cum_returns = returns.cumsum()
        return -1.0*(max(cum_returns) - cum_returns[-1]) 

    # Calculates the global maximum drawdown i.e. the maximum drawdown till now
    def drawdown(self,returns):
        if returns.shape[0] < 2:
            return 0.0
        cum_returns = returns.cumsum()
        return -1.0*max(maximum.accumulate(cum_returns) - cum_returns) # Will return a negative value

    def rollsum(self,series,period):
        n = series.shape[0]
        if(n<period):
            return array([]) #empty array
        return array([sum(series[i:i+period]) for i in xrange(0,n-period+1)]).astype(float)

    def mean_lowest_k_percent(self,series,k):
        sorted_series = sort(series)
        n = sorted_series.shape[0]
        _retval=0
        if n <= 0 :
            _retval=0
        else:
            _index_of_worst_k_percent = int((k/100.0)*n)
            if _index_of_worst_k_percent <= 0:
                _retval=sorted_series[0]
            else:
                _retval=mean(sorted_series[0:_index_of_worst_k_percent])
        return _retval

    # Prints the returns for k worst and k best days
    def print_extreme_days(self, k):
        _dates_returns = zip(self.dates,self.daily_log_returns)
        _sorted_returns = sorted(_dates_returns, key = lambda x: x[1]) # Sort by returns
        _end_index_worst_days = min(len(_sorted_returns), k)
        _start_index_best_days = max(0, len(_sorted_returns) - k)
        if len(_sorted_returns) > 0:
            _worst_days = _sorted_returns[0:_end_index_worst_days]
            _best_days = _sorted_returns[_start_index_best_days:len(_sorted_returns)]
            print '\nWorst %d Days:'%k
            for item in _worst_days:
                print item[0],' : ',(exp(item[1])-1)*100.0,'%'
            print '\nBest %d Days:'%k
            for item in reversed(_best_days):
                print item[0],' : ',(exp(item[1])-1)*100.0,'%'

    # non public function to save results to a file
    def _save_results(self):
        with open(self.returns_file, 'wb') as f:
            pickle.dump(zip(self.dates,self.daily_log_returns), f)

    def show_results(self):
        self.PnL = sum(self.PnLvector) # final sum of pnl of all trading days
        self.net_returns = ( self.PnL*100.0 )/self.initial_capital # final sum of pnl / initial capital
        self.annualized_PnL = 252.0 * mean(self.PnLvector)
        self.annualized_stdev_PnL = sqrt(252.0)*std(self.PnLvector)
        self.daily_returns = self.PnLvector*100.0/self.value[0:self.value.shape[0]-1]
        monthly_log_returns = self.rollsum(self.daily_log_returns,21)
        quarterly_log_returns = self.rollsum(self.daily_log_returns,63)
        yearly_log_returns = self.rollsum(self.daily_log_returns,252)
        self._monthly_nominal_returns_percent = (exp(monthly_log_returns)-1)*100
        self._quarterly_nominal_returns_percent = (exp(quarterly_log_returns)-1)*100
        self._yearly_nominal_returns_percent = (exp(yearly_log_returns)-1)*100
        self.dml = (exp(self.mean_lowest_k_percent(self.daily_log_returns,10))-1)*100.0
        self.mml = (exp(self.mean_lowest_k_percent(monthly_log_returns,10))-1)*100.0
        self._worst_10pc_quarterly_returns = (exp(self.mean_lowest_k_percent(quarterly_log_returns,10))-1)*100.0
        self._worst_10pc_yearly_returns = (exp(self.mean_lowest_k_percent(yearly_log_returns,10))-1)*100.0
        self._annualized_returns_percent = (exp(252.0*mean(self.daily_log_returns))-1)*100.0
        self.annualized_stddev_returns = (exp(sqrt(252.0)*std(self.daily_log_returns))-1)*100.0
        self.sharpe = self._annualized_returns_percent/self.annualized_stddev_returns
        self.skewness = ss.skew(self.daily_log_returns)
        self.kurtosis = ss.kurtosis(self.daily_log_returns)
        max_dd_log = self.drawdown(self.daily_log_returns)
        self.max_drawdown_percent = abs((exp(max_dd_log)-1)*100)
        self.max_drawdown_dollar = abs(self.drawdown(self.PnLvector))
        self.return_by_maxdrawdown = self._annualized_returns_percent/self.max_drawdown_percent
        self._annualized_pnl_by_max_drawdown_dollar = self.annualized_PnL/self.max_drawdown_dollar
        self.print_extreme_days(10)
        self._save_results()

        print "\nInitial Capital = %.10f\nNet PNL = %.10f \nTrading Cost = %.10f\nNet Returns = %.10f%%\nAnnualized PNL = %.10f\nAnnualized_Std_PnL = %.10f\nAnnualized_Returns = %.10f%% \nAnnualized_Std_Returns = %.10f%% \nSharpe Ratio = %.10f \nSkewness = %.10f\nKurtosis = %.10f\nDML = %.10f%%\nMML = %.10f%%\nQML = %.10f%%\nYML = %.10f%%\nMax Drawdown = %.10f%% \nMax Drawdown Dollar = %.10f \nAnnualized PNL by drawdown = %.10f \nReturn_drawdown_Ratio = %.10f \n" %(self.initial_capital,self.PnL,self.trading_cost,self.net_returns,self.annualized_PnL,self.annualized_stdev_PnL,self._annualized_returns_percent,self.annualized_stddev_returns,self.sharpe,self.skewness,self.kurtosis,self.dml,self.mml,self._worst_10pc_quarterly_returns,self._worst_10pc_yearly_returns,self.max_drawdown_percent,self.max_drawdown_dollar,self._annualized_pnl_by_max_drawdown_dollar,self.return_by_maxdrawdown)
