from BackTester.BackTester_Listeners import BackTesterListener
from BackTester.BackTester import BackTester
from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import EventsListener
from Utils.Regular import checkEOD,getdtfromdate
from Utils.DbQueries import conv_factor
from BookBuilder.BookBuilder import BookBuilder
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

#Performance tracker listens to the Dispatcher for concurrent events so that it can record the daily returns
#It also listens to the Backtester for any new filled orders
#At the end it shows the results and plot the cumulative PnL graph
#It outputs the list of [dates,dailyreturns] to returns_file for later analysis
#It outputs the portfolio snapshots and orders in the positions_file for debugging
class PerformanceTracker(BackTesterListener,EventsListener):

    instance=[]

    def __init__(self,products,config_file):
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))
        start_date = config.get('Dates', 'start_date')
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
        self.value = array([self.initial_capital])                                                                   #Track end of day values of the portfolio
        self.PnLvector = empty(shape=(0))
        self.annualized_PnL = 0
        self.annualized_stdev_PnL = 0
        self.annualized_returns = 0
        self.annualized_stddev_returns = 0
        self.sharpe = 0
        self.daily_returns = empty(shape=(0))
        self.monthly_returns = empty(shape=(0))
        self.quaterly_returns = empty(shape=(0))
        self.yearly_returns = empty(shape=(0))
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
        self.portfolio_snapshots = []
        self.total_orders = 0
        dispatcher = Dispatcher.get_unique_instance(products,config_file)
        dispatcher.AddEventsListener(self)
        for product in products:
            backtester = BackTester.get_unique_instance(product,config_file)
            backtester.AddListener(self)

        self.bb_objects={}
        for product in products:
            self.bb_objects[product] = BookBuilder.get_unique_instance(product,config_file)

    @staticmethod
    def get_unique_instance(products,config_file):
        if(len(PerformanceTracker.instance)==0): # till now, no PerformanceTracker objects have been created
            new_instance = PerformanceTracker(products,config_file)
            PerformanceTracker.instance.append(new_instance)
        return PerformanceTracker.instance[0] # if there is an object already, then it returns that object

    def OnOrderUpdate(self,filled_orders,current_date):
        for order in filled_orders:
            self.cash = self.cash - order['value'] - order['cost']
            self.num_shares[order['product']] = self.num_shares[order['product']] + order['amount']
        self.PrintFilledOrders(filled_orders)

    def AfterSettlementDay(self,product):
        p1 = product
        p2 = product.rstrip('1')+'2'
        assert self.num_shares[p1]==0
        self.num_shares[p1]=self.num_shares[p2]
        self.num_shares[p2]=0

    def OnEventsUpdate(self,events):
        all_EOD = checkEOD(events)                                                              #check whether all the events are ENDOFDAY
        if(all_EOD):
            self.ComputeDailyStats(events[0]['dt'].date())
            self.PrintSnapshot(events[0]['dt'].date())

    def getPortfolioValue(self):
        netValue = self.cash
        for product in self.products:
            if(self.num_shares[product]!=0):
                book = self.bb_objects[product].dailybook
                current_price = book[-2][1]                                                        #Yesterday's price
                netValue = netValue + current_price*self.num_shares[product]*self.conversion_factor[product]
        return netValue

    def ComputeDailyStats(self,date):
        todaysValue = self.getPortfolioValue()
        self.value = append(self.value,todaysValue)
        self.PnLvector = append(self.PnLvector,self.value[-1]-self.value[-2])                     #daily PnL = Value of portfolio on last day - Value of portfolio on 2nd last day
        self.dates.append(date)

    def PrintFilledOrders(self,filled_orders):
        if(len(filled_orders)==0): return
        s = 'ORDER FILLED : '
        for order in filled_orders:
            s = s + 'product:%s amount:%f cost:%f value:%f'%(order['product'],order['amount'],order['cost'],order['value'])
        text_file = open(self.positions_file, "a")
        text_file.write("%s\n" % s)
        text_file.close()

    def PrintSnapshot(self,date):
        text_file = open(self.positions_file, "a")
        text_file.write("\nPortfolio snapshot at StartOfDay %s\nCash:%f\tPositions:%s\n\n" % (date,self.cash,str(self.num_shares)))
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

    def _save_results(self,dates,daily_log_returns):
        with open(self.returns_file, 'wb') as f:
            pickle.dump(zip(dates,daily_log_returns), f)

    def showResults(self):
        self.PnL = self.getPortfolioValue() - self.initial_capital
        self.net_returns = self.PnL*100.0/self.initial_capital
        self.annualized_PnL = 252.0 * mean(self.PnLvector)
        self.annualized_stdev_PnL = sqrt(252.0)*std(self.PnLvector)
        self.daily_returns = self.PnLvector*100.0/self.value[0:self.value.shape[0]-1]
        daily_log_returns = log(1 + self.daily_returns/100.0)
        monthly_log_returns = self.rollsum(daily_log_returns,21)
        quaterly_log_returns = self.rollsum(daily_log_returns,63)
        yearly_log_returns = self.rollsum(daily_log_returns,252)
        self.monthly_returns = (exp(monthly_log_returns)-1)*100
        self.quaterly_returns = (exp(quaterly_log_returns)-1)*100
        self.yearly_returns = (exp(yearly_log_returns)-1)*100
        self.dml = (exp(self.meanlowestkpercent(daily_log_returns,10))-1)*100.0
        self.mml = (exp(self.meanlowestkpercent(monthly_log_returns,10))-1)*100.0
        self.qml = (exp(self.meanlowestkpercent(quaterly_log_returns,10))-1)*100.0
        self.yml = (exp(self.meanlowestkpercent(yearly_log_returns,10))-1)*100.0
        self.annualized_returns = (exp(252.0*mean(daily_log_returns))-1)*100.0
        self.annualized_stddev_returns = (exp(sqrt(252.0)*std(daily_log_returns))-1)*100.0
        self.sharpe = self.annualized_returns/self.annualized_stddev_returns
        self.skewness = ss.skew(self.PnLvector)
        self.kurtosis = ss.kurtosis(self.PnLvector)
        max_dd_log = self.drawdown(daily_log_returns)
        self.max_drawdown_percent = (exp(max_dd_log)-1)*100
        self.max_drawdown_dollar = self.drawdown(self.PnLvector)
        self.return_by_maxdrawdown = self.annualized_returns/self.max_drawdown_percent
        self.annualizedPnLbydrawdown = self.annualized_PnL/self.max_drawdown_dollar

        self._save_results(self.dates,daily_log_returns)

        print "\n-------------RESULTS--------------------\nInitial Capital = %.10f\nNet PNL = %.10f \nNet Returns = %.10f%%\nAnnualized PNL = %.10f\nAnnualized_Std_PnL = %.10f\nAnnualized_Returns = %.10f%% \nAnnualized_Std_Returns = %.10f%% \nSharpe Ratio = %.10f \nSkewness = %.10f\nKurtosis = %.10f\nDML = %.10f%%\nMML = %.10f%%\nQML = %.10f%%\nYML = %.10f%%\nMax Drawdown = %.10f%% \nMax Drawdown Dollar = %.10f \nAnnualized PNL by drawdown = %.10f \nReturn_drawdown_Ratio = %.10f \n" %(self.initial_capital,self.PnL,self.net_returns,self.annualized_PnL,self.annualized_stdev_PnL,self.annualized_returns,self.annualized_stddev_returns,self.sharpe,self.skewness,self.kurtosis,self.dml,self.mml,self.qml,self.yml,self.max_drawdown_percent,self.max_drawdown_dollar,self.annualizedPnLbydrawdown,self.return_by_maxdrawdown)

        self.PlotPnLVersusDates(self.dates,array(self.PnLvector).astype(float))
