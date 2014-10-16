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

'''Performance tracker listens to the Dispatcher for concurrent events so that it can record the daily returns
 It also listens to the Backtester for any new filled orders
 At the end it shows the results and plot the cumulative PnL graph
 It outputs the list of [dates,dailyreturns] to returns_file for later analysis
 It outputs the portfolio snapshots and orders in the positions_file for debugging 
'''
class PerformanceTracker(BackTesterListener,EndOfDayListener):

    def __init__(self,products,config_file):
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))
        self.date = get_dt_from_date(config.get('Dates', 'start_date')).date()  #The earliest date for which daily stats still need to be computed
        self.positions_file = config.get('Files','positions_file')
        self.returns_file = config.get('Files','returns_file')
        self.initial_capital = config.getfloat('Parameters', 'initial_capital')
        self.cash = self.initial_capital
        self.products = products
        self.conversion_factor = conv_factor(products)
        self.strategy_name = config.get('Strategy','name')
        self.num_shares = {}
        for product in products:
            self.num_shares[product] = 0
        self.dates = []
        self.PnL = 0
        self.net_returns = 0
        self.value = array([self.initial_capital])  # Track end of day values of the portfolio
        self.PnLvector = empty(shape=(0))
        self.annualized_PnL = 0
        self.annualized_stdev_PnL = 0
        self.annualized_returns = 0
        self.annualized_stddev_returns = 0
        self.sharpe = 0
        self.daily_returns = empty ( shape=(0) )
        self.daily_log_returns = empty ( shape=(0) )
        self.monthly_returns = empty ( shape=(0) )
        self.quaterly_returns = empty ( shape=(0) )
        self.yearly_returns = empty ( shape=(0) )
        self.dml = 0
        self.mml = 0
        self.qml = 0
        self.yml = 0
        self.max_drawdown_percent = 0
        self.max_drawdown_dollar = 0
        self.return_by_maxdrawdown = 0
        self.annualizedPnLbydrawdown = 0
        self.skewness = 0
        self.kurtosis = 0
        self.trading_cost = 0 
        self.portfolio_snapshots = []
        self.total_orders = 0
        dispatcher = Dispatcher.get_unique_instance(products,config_file)
        dispatcher.add_end_of_day_listener(self)
        for product in products:
            backtester = BackTester.get_unique_instance(product,config_file)
            backtester.add_listener(self)

        self.bb_objects={}
        for product in products:
            self.bb_objects[product] = BookBuilder.get_unique_instance(product,config_file)

    def on_order_update(self,filled_orders,current_date):
        for order in filled_orders:
            self.cash = self.cash - order['value'] - order['cost']
            self.num_shares[order['product']] = self.num_shares[order['product']] + order['amount']
            self.trading_cost = self.trading_cost + order['cost']
            self.total_orders = self.total_orders+1
        self.print_filled_orders(filled_orders)

    def after_settlement_day(self,product):
        p1 = product
        p2 = product.rstrip('1')+'2'
        assert self.num_shares[p1]==0
        self.num_shares[p1]=self.num_shares[p2]
        self.num_shares[p2]=0

    # Called by Dispatcher
    def on_end_of_day(self,date):
        # TODO { } probably more efficient to compute and send by dispatcher ?
        self.compute_daily_stats(date)
        self.print_snapshot(date)

    # Computes the portfolio value at ENDOFDAY on 'date'
    def get_portfolio_value(self,date):
        s = ''
        netValue = self.cash
        for product in self.products:
            if(self.num_shares[product]!=0):
                book = self.bb_objects[product].dailybook
                if(len(book)>=2 and date==book[-2][0].date()):
                    current_price = book[-2][1]  # If the date matches use the price
                elif(len(book) >= 1):
                    current_price = book[-1][1]  # Else use the latest price(date may not be the same)
                else:
                   sys.exit('DailyBook length 0')  # Should not reach here
                s = s + product + ' ' + str(current_price)
                netValue = netValue + current_price*self.num_shares[product]*self.conversion_factor[product]
        self.print_prices(date,s)  
        return netValue

    def print_prices(self,date,s):
        text_file = open(self.positions_file, "a")
        text_file.write("Prices at end of day %s are: %s\n" % (date,s))
        text_file.close()

    # Computes the daily stats for the most recent trading day prior to 'date'
    def compute_daily_stats(self,date):
        if(self.date < date and self.total_orders>0): #If no orders have been filled,it implies trading has not started yet
            todaysValue = self.get_portfolio_value(self.date)
            self.value = append(self.value,todaysValue)
            self.PnLvector = append(self.PnLvector,self.value[-1]-self.value[-2])  # daily PnL = Value of portfolio on last day - Value of portfolio on 2nd last day
            self.daily_log_returns = append ( self.daily_log_returns, log ( self.value[-1] / self.value[-2] ) )
            # TODO {gchak} check if the number is negative.
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
        return max(maximum.accumulate(cum_returns) - cum_returns)

    def rollsum(self,series,period):
        n = series.shape[0]
        if(n<period):
            return array([])                                                                             #empty array
        return array([sum(series[i:i+period]) for i in xrange(0,n-period+1)]).astype(float)

    def meanlowestkpercent(self,series,k):
        sorted_series = sort(series)
        n = sorted_series.shape[0]
        num = int((k/100.0)*n)
        return mean(sorted_series[0:num])

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

    def _save_results(self):
        with open(self.returns_file, 'wb') as f:
            pickle.dump(zip(self.dates,self.daily_log_returns), f)

    def showResults(self):
        self.PnL = sum(self.PnLvector)
        self.net_returns = self.PnL*100.0/self.initial_capital
        self.annualized_PnL = 252.0 * mean(self.PnLvector)
        self.annualized_stdev_PnL = sqrt(252.0)*std(self.PnLvector)
        self.daily_returns = self.PnLvector*100.0/self.value[0:self.value.shape[0]-1]
        monthly_log_returns = self.rollsum(self.daily_log_returns,21)
        quaterly_log_returns = self.rollsum(self.daily_log_returns,63)
        yearly_log_returns = self.rollsum(self.daily_log_returns,252)
        self.monthly_returns = (exp(monthly_log_returns)-1)*100
        self.quaterly_returns = (exp(quaterly_log_returns)-1)*100
        self.yearly_returns = (exp(yearly_log_returns)-1)*100
        self.dml = (exp(self.meanlowestkpercent(self.daily_log_returns,10))-1)*100.0
        self.mml = (exp(self.meanlowestkpercent(monthly_log_returns,10))-1)*100.0
        self.qml = (exp(self.meanlowestkpercent(quaterly_log_returns,10))-1)*100.0
        self.yml = (exp(self.meanlowestkpercent(yearly_log_returns,10))-1)*100.0
        self.annualized_returns = (exp(252.0*mean(self.daily_log_returns))-1)*100.0
        self.annualized_stddev_returns = (exp(sqrt(252.0)*std(self.daily_log_returns))-1)*100.0
        self.sharpe = self.annualized_returns/self.annualized_stddev_returns
        self.skewness = ss.skew(self.daily_log_returns)
        self.kurtosis = ss.kurtosis(self.daily_log_returns)
        max_dd_log = self.drawdown(self.daily_log_returns)
        self.max_drawdown_percent = (exp(max_dd_log)-1)*100
        self.max_drawdown_dollar = self.drawdown(self.PnLvector)
        self.return_by_maxdrawdown = self.annualized_returns/self.max_drawdown_percent
        self.annualizedPnLbydrawdown = self.annualized_PnL/self.max_drawdown_dollar

        self._save_results()

        print "\n-------------RESULTS--------------------\nInitial Capital = %.10f\nNet PNL = %.10f \nTrading Cost = %.10f\nNet Returns = %.10f%%\nAnnualized PNL = %.10f\nAnnualized_Std_PnL = %.10f\nAnnualized_Returns = %.10f%% \nAnnualized_Std_Returns = %.10f%% \nSharpe Ratio = %.10f \nSkewness = %.10f\nKurtosis = %.10f\nDML = %.10f%%\nMML = %.10f%%\nQML = %.10f%%\nYML = %.10f%%\nMax Drawdown = %.10f%% \nMax Drawdown Dollar = %.10f \nAnnualized PNL by drawdown = %.10f \nReturn_drawdown_Ratio = %.10f \n" %(self.initial_capital,self.PnL,self.trading_cost,self.net_returns,self.annualized_PnL,self.annualized_stdev_PnL,self.annualized_returns,self.annualized_stddev_returns,self.sharpe,self.skewness,self.kurtosis,self.dml,self.mml,self.qml,self.yml,self.max_drawdown_percent,self.max_drawdown_dollar,self.annualizedPnLbydrawdown,self.return_by_maxdrawdown)

        self.PlotPnLVersusDates(self.dates,array(self.PnLvector).astype(float))
