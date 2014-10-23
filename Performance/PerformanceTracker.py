import os
import datetime
import ConfigParser
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
from Utils.Regular import check_eod,get_dt_from_date
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
class PerformanceTracker(BackTesterListener,EndOfDayListener):

    def __init__(self,products,config_file):
        _config = ConfigParser.ConfigParser() # TODO {design} Uneasy with the config file being passed directly. Shouldn't we pass a struct with values ?
        _config.readfp(open(config_file,'r'))
        self.date = get_dt_from_date(_config.get('Dates', 'start_date')).date()  #The earliest date for which daily stats still need to be computed
        self.positions_file = 'positions_' + os.path.splitext(config_file)[0].split('/')[-1] +'.txt'
        self.returns_file = 'returns_' + os.path.splitext(config_file)[0].split('/')[-1] +'.txt'
        open(self.returns_file,'w').close()
        self.initial_capital = _config.getfloat('Parameters', 'initial_capital')
        self.cash = self.initial_capital
        self.products = products
        self.conversion_factor = conv_factor(products)
        self.strategy_name = _config.get('Strategy','name')
        self.num_shares = {}
        for product in products:
            self.num_shares[product] = 0
        self.num_shares_traded = {}
        for product in products:
            self.num_shares_traded[product] = 0
        self.dates = []
        self.PnL = 0
        self.net_returns = 0
        self.value = array([self.initial_capital])  # Track end of day values of the portfolio
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
        dispatcher = Dispatcher.get_unique_instance(products,config_file)
        dispatcher.add_end_of_day_listener(self)

        # Currently listens to replies from backtester directly
        for product in products:
            backtester = BackTester.get_unique_instance(product,config_file)
            backtester.add_listener(self)

        self.bb_objects={}
        for product in products:
            self.bb_objects[product] = BookBuilder.get_unique_instance(product,config_file)

    def on_order_update(self,filled_orders,current_date):
        for order in filled_orders:
            # TODO {gchak} change the framework for futures. We have currently assumed futures to be fully cash settled I believe.
            # i.e. We have to pay the full notional valu in cash
            self.cash = self.cash - order['value'] - order['cost']

            # TODO {gchak} Coming from a C++ world, I can't help but wonder about the time complexity of order['amount'] versus order.amount
            self.num_shares[order['product']] = self.num_shares[order['product']] + order['amount']
            self.num_shares_traded[order['product']] = self.num_shares_traded[order['product']] + abs(order['amount'])
            self.trading_cost = self.trading_cost + order['cost']
            self.total_orders = self.total_orders+1
        self.print_filled_orders(filled_orders)

    # TOASK {gchak} This function is not being called anywhere. So maybe this question isn't relevant any more, but I was wondering if we need this ?
    # All we need to do after settlement day is to consider the impact of settlement of contratcs that we held to settlement.
    def after_settlement_day(self,product):
        p1 = product.rstrip('12')+'1'
        p2 = product.rstrip('12')+'2'
        if(self.num_shares[p1]==0 and self.num_shares[p2]!=0):
            self.num_shares[p1] = self.num_shares[p2]
            self.num_shares[p2] = 0

    # Called by Dispatcher
    def on_end_of_day(self,date):
        self.compute_daily_stats(date)
        self.print_snapshot(date)

    #Find the latest price prior to 'date'
    def find_most_recent_price(self,book,date):
        if(len(book)<1):
            sys.exit('ERROR: warmupdays not sufficient')
        elif(book[-1][0].date()<=date):
            return book[-1][1]
        else:
            return self.find_most_recent_price(book[:-1],date)

    #Find the latest price prior to 'date' for futures product
    def find_most_recent_price_future(self,book1,book2,date):
        if(len(book1)<1):
            sys.exit('ERROR: warmupdays not sufficient')
        elif(book1[-1][0].date()<=date and book1[-1][2]): #If the day was settlement day,then use second futures contract price
            return book2[-1][1]
        elif(book1[-1][0].date()<=date and not book1[-1][2]): #If the day was not settlement day,then use first futures contract price
            return book1[-1][1]
        else:
            return self.find_most_recent_price_future(book1[:-1],book2[:-1],date)

    # Computes the portfolio value at ENDOFDAY on 'date'
    def get_portfolio_value(self,date):
        netValue = self.cash
        for product in self.products:
            if(self.num_shares[product]!=0):
                if(product[0]=='f'):
                    product1 = product
                    product2 = product.rstrip('1')+'2'
                    book1 = self.bb_objects[product1].dailybook
                    book2 = self.bb_objects[product2].dailybook
                    current_price = self.find_most_recent_price_future(book1,book2,date)
                else:
                    book = self.bb_objects[product].dailybook
                    current_price = self.find_most_recent_price(book,date)
                netValue = netValue + current_price*self.num_shares[product]*self.conversion_factor[product]
        return netValue

    def print_prices(self,date):
        s = ''
        for product in self.products:
            if(self.num_shares_traded[product]!=0):
                if(product[0]=='f'):
                    product1 = product
                    product2 = product.rstrip('1')+'2' # TODO { gchak } not working
                    book1 = self.bb_objects[product1].dailybook
                    book2 = self.bb_objects[product2].dailybook
                    current_price = self.find_most_recent_price_future(book1,book2,date)
                else:
                    book = self.bb_objects[product].dailybook
                    current_price = self.find_most_recent_price(book,date)
                s = s + product + ' ' + str(current_price)
        text_file = open(self.positions_file, "a") # TODO {gchak} consider opening file once, and sharing the same file pointer among all the PerformanceTracker objects
        text_file.write("Prices at end of day %s are: %s\n" % (date,s))
        text_file.close()

    # Computes the daily stats for the most recent trading day prior to 'date'
    # TOASK {gchak} Do we ever expect to run this function without current date ?
    def compute_daily_stats(self,date):
        if(self.date < date and self.total_orders>0): #If no orders have been filled,it implies trading has not started yet
            todaysValue = self.get_portfolio_value(self.date)
            # self.print_prices(self.date) # TODO Only if needed. For instance in zero-logging mode or optimization mode, we will want to disable this.
            self.value = append ( self.value, todaysValue )
            self.PnLvector = append ( self.PnLvector, ( self.value[-1]-self.value[-2] ) )  # daily PnL = Value of portfolio on last day - Value of portfolio on 2nd last day

            if ( self.value[-1] <= 0 ) :
                _logret_today = -1000; # real value is -inf
            else :
                _logret_today = log ( self.value[-1] / self.value[-2] )
            self.daily_log_returns = append ( self.daily_log_returns, _logret_today )

            self.dates.append(self.date)
        self.date=date

    def print_filled_orders(self,filled_orders):
        if(len(filled_orders)==0): return
        s = 'ORDER FILLED : '
        for order in filled_orders:
            s = s + 'product:%s amount:%f cost:%f value:%f fill_price:%f'%(order['product'],order['amount'],order['cost'],order['value'],order['fill_price'])
        text_file = open(self.positions_file, "a")
        text_file.write("%s\n" % s)
        text_file.close()

    def print_snapshot(self,date):
        text_file = open(self.positions_file, "a")
        if(self.PnLvector.shape[0]>0):
            s = ("\nPortfolio snapshot at StartOfDay %s\nCash:%f\tPositions:%s Portfolio Value:%f PnL for last trading day:%f \n\n" % (date,self.cash,str(self.num_shares),self.value[-1],self.PnLvector[-1]))
        else:
            s = ("\nPortfolio snapshot at StartOfDay %s\nCash:%f\tPositions:%s Portfolio Value:%f\n\n" % (date,self.cash,str(self.num_shares),self.value[-1]))
        text_file.write(s)
        text_file.close()

    def drawdown(self,returns):
        cum_returns = returns.cumsum()
        return -1.0*max(maximum.accumulate(cum_returns) - cum_returns) # Will return a negative value

    def rollsum(self,series,period):
        n = series.shape[0]
        if(n<period):
            return array([]) #empty array
        return array([sum(series[i:i+period]) for i in xrange(0,n-period+1)]).astype(float)

    def meanlowestkpercent(self,series,k):
        sorted_series = sort(series)
        n = sorted_series.shape[0]
        _retval=0
        if n <= 0 :
            _retval=0
        _index_of_worst_k_percent = int((k/100.0)*n)
        if _index_of_worst_k_percent <= 0:
            _retval=sorted_series[0]
        else:
            _retval=mean(sorted_series[0:_index_of_worst_k_percent])
        return _retval

    # TODO {sanchit} move plotting outside to seaprate utilities or charting modules.
    # Print data in separate files to be amenable to easy plotting.
    # This will allow us to use more customized solutions.
    # We might want to write separate files for dailyPnL or daily_log_returns
    def PlotPnLVersusDates(self,dates,dailyPnL):
        num = int(len(dates)/5.0)
        if(num==0): num=1
        for i in xrange(0,len(dates)):
            if(i%num!=0 and i!= len(dates)-1):
                dates[i]=''
            else:
                dates[i] = dates[i].strftime('%d/%m/%Y')
        plt.plot(dailyPnL.cumsum())
        plt.xticks(range(len(dailyPnL)),dates)
        plt.xlabel('Date')
        plt.ylabel('Cumulative PnL')
        plt.savefig('Cumulative_PnL_'+self.strategy_name.split('.')[0]+".png", bbox_inches='tight')

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
        self.dml = (exp(self.meanlowestkpercent(self.daily_log_returns,10))-1)*100.0
        self.mml = (exp(self.meanlowestkpercent(monthly_log_returns,10))-1)*100.0
        self._worst_10pc_quarterly_returns = (exp(self.meanlowestkpercent(quarterly_log_returns,10))-1)*100.0
        self._worst_10pc_yearly_returns = (exp(self.meanlowestkpercent(yearly_log_returns,10))-1)*100.0
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

        self._save_results()

        print "\n-------------RESULTS--------------------\nInitial Capital = %.10f\nNet PNL = %.10f \nTrading Cost = %.10f\nNet Returns = %.10f%%\nAnnualized PNL = %.10f\nAnnualized_Std_PnL = %.10f\nAnnualized_Returns = %.10f%% \nAnnualized_Std_Returns = %.10f%% \nSharpe Ratio = %.10f \nSkewness = %.10f\nKurtosis = %.10f\nDML = %.10f%%\nMML = %.10f%%\nQML = %.10f%%\nYML = %.10f%%\nMax Drawdown = %.10f%% \nMax Drawdown Dollar = %.10f \nAnnualized PNL by drawdown = %.10f \nReturn_drawdown_Ratio = %.10f \n" %(self.initial_capital,self.PnL,self.trading_cost,self.net_returns,self.annualized_PnL,self.annualized_stdev_PnL,self._annualized_returns_percent,self.annualized_stddev_returns,self.sharpe,self.skewness,self.kurtosis,self.dml,self.mml,self._worst_10pc_quarterly_returns,self._worst_10pc_yearly_returns,self.max_drawdown_percent,self.max_drawdown_dollar,self._annualized_pnl_by_max_drawdown_dollar,self.return_by_maxdrawdown)

        self.PlotPnLVersusDates(self.dates,array(self.PnLvector).astype(float))
