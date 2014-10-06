#!/usr/bin/python

from numpy import *
import scipy.stats as ss
import datetime
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pylab

class PerformanceTracker:
    def __init__(self,initial_capital,products,conversion_factor):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.products = products
        self.conversion_factor = conversion_factor
        self.num_shares = {}
        for product in products:
            self.num_shares[product] = 0
        self.dates = []
        self.PnL = 0                                                                                     #
        self.net_returns = 0    
        self.value = [initial_capital]                                                                   #Track end of day values of the portfolio
        self.PnLvector = []                            
        self.annualized_PnL = 0
        self.annualized_returns = 0
        self.annualized_stddev = 0
        self.sharpe = 0
        self.daily_returns = []
        self.monthly_returns = []
        self.quaterly_returns = []
        self.yearly_returns = []
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
  
    def getPortfolioValue(self):
        netValue = self.cash
        for product in self.products:
            if(self.num_shares[product]!=0): 
                book = self.bb_objects[product].dailybook
                current_price = book[len(book)-1][1]
                netValue = netValue + current_price*self.num_shares[product]*self.conversion_factor[product]
        return netValue

    def analyze(self,filled_orders,current_date):
        self.dailyStats(current_date)
        for order in filled_orders:
            self.cash = self.cash - order['value'] - order['cost']
            self.num_shares[order['product']] = self.num_shares[order['product']] + order['amount']
           
    def dailyStats(self,current_date):
        if(self.date < current_date):
            todaysValue = self.getPortfolioValue()
            self.value.append(todaysValue)
            self.PnLvector.append(self.value[-1]-self.value[-2])                                   #daily PnL = Value of portfolio on last day - Value of portfolio on 2nd last day
            self.dates.append(self.date)
            self.date=current_date

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
        num = int(len(dates)/5)
        for i in xrange(0,len(dates)):
            if(i%num!=0 and i!= len(dates)-1):
                dates[i]=''
            else:
                dates[i] = dates[i].strftime('%d/%m/%Y')  
        plt.plot(dailyPnL.cumsum())
        plt.xticks(range(len(dailyPnL)),dates)
        plt.savefig('Cumulative_PnL.png', bbox_inches='tight')

    def showResults(self):
        self.PnL = self.getPortfolioValue() - self.initial_capital
        self.net_returns = self.PnL*100.0/self.initial_capital
        self.PnLvector = array(self.PnLvector).astype(float)
        self.value = array(self.value).astype(float)
        self.annualized_PnL = 252.0 * mean(self.PnLvector)
        self.daily_returns = self.PnLvector*100.0/self.value[0:self.value.shape[0]-1]
        daily_log_returns = log(1 + self.daily_returns/100.0)
        monthly_log_returns = self.rollsum(daily_log_returns,21)
        quaterly_log_returns = self.rollsum(daily_log_returns,63)
        yearly_log_returns = self.rollsum(daily_log_returns,252)
        self.monthly_returns = (exp(monthly_log_returns)-1)*100
        self.quaterly_returns = (exp(quaterly_log_returns)-1)*100
        self.yearly_returns = (exp(yearly_log_returns)-1)*100
        self.dml = self.meanlowestkpercent(self.daily_returns,10)                                  #check if we should use log returns
        self.mml = self.meanlowestkpercent(self.monthly_returns,10)
        self.qml = self.meanlowestkpercent(self.quaterly_returns,10)
        self.yml = self.meanlowestkpercent(self.yearly_returns,10)
        self.annualized_returns = 252.0 * mean(self.daily_returns)                                 #check if we should use log returns
        self.annualized_stddev = sqrt(252.0)*std(self.daily_returns)                               #check if we should use log returns
        self.sharpe = self.annualized_returns/self.annualized_stddev
        self.skewness = ss.skew(self.daily_returns)
        self.kurtosis = ss.kurtosis(self.daily_returns)
        max_dd_log = self.drawdown(daily_log_returns)
        self.max_drawdown_percent = (exp(max_dd_log)-1)*100
        self.max_drawdown_dollar = self.drawdown(self.PnLvector)
        self.return_by_maxdrawdown = self.annualized_returns/self.max_drawdown_percent
        self.annualizedPnLbydrawdown = self.annualized_PnL/self.max_drawdown_dollar 

        print "\n-------------RESULTS--------------------\nNet PNL = %.10f \nNet Returns = %.10f%%\nAnnualized PNL = %.10f\nAnnualized_Returns = %.10f%% \nAnnualized_Std = %.10f%% \nSharpe Ratio = %.10f \nSkewness = %.10f\nKurtosis = %.10f\nDML = %.10f%%\nMML = %.10f%%\nQML = %.10f%%\nYML = %.10f%%\nMax Drawdown = %.10f%% \nMax Drawdown Dollar = %.10f \nAnnualized PNL by drawdown = %.10f \nReturn_drawdown_Ratio = %.10f \n" %(self.PnL,self.net_returns,self.annualized_PnL,self.annualized_returns,self.annualized_stddev,self.sharpe,self.skewness,self.kurtosis,self.dml,self.mml,self.qml,self.yml,self.max_drawdown_percent,self.max_drawdown_dollar,self.annualizedPnLbydrawdown,self.return_by_maxdrawdown)

        self.PlotPnLVersusDates(self.dates,array(self.PnLvector).astype(float))
