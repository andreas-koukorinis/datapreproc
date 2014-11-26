import sys
import os
import datetime
from numpy import *
import scipy.stats as ss
import datetime
import pickle
from Utils.Regular import get_dt_from_date
from DailyLogReturns import DailyLogReturns
from Indicator_Listeners import IndicatorListener

class SignalPerformanceTracker(IndicatorListener):

    def __init__(self, products, _startdate, _enddate, _config):
        self.products = products
        self.date = get_dt_from_date(_startdate).date()
        self.latest_logret = {}
        self.dates = []
        self.PnL = 0
        self.net_returns = 0
        self.initial_capital = 1.0
        self.value = array([self.initial_capital])  # Track end of day values of the portfolio
        self.PnLvector = empty(shape=(0))
        self.annualized_PnL = 0
        self.annualized_stdev_PnL = 0
        self._annualized_returns_percent = 0
        self.annualized_stddev_returns = 0
        self.sharpe = 0
        self.daily_returns = empty(shape=(0))
        self.daily_log_returns = empty(shape=(0))
        self._monthly_nominal_returns_percent = empty(shape=(0))
        self._quarterly_nominal_returns_percent = empty(shape=(0))
        self._yearly_nominal_returns_percent = empty(shape=(0))
        self.dml = 0
        self.mml = 0
        self._worst_10pc_quarterly_returns = 0
        self._worst_10pc_yearly_returns = 0
        self.current_drawdown = 0
        self.max_drawdown_percent = 0
        self.max_drawdown_dollar = 0
        self.return_by_maxdrawdown = 0
        self._annualized_pnl_by_max_drawdown_dollar = 0
        self.skewness = 0
        self.kurtosis = 0
        self.trading_cost = 0
        for product in self.products:
            DailyLogReturns.get_unique_instance('DailyLogReturns.' + product, _startdate, _enddate, _config).add_listener(self)
            self.latest_logret[product] = (self.date, 0.0) # Initial dummy value for each product

    def on_indicator_update(self, identifier, daily_log_returns_dt):
        product = identifier.split('.')[1]
        self.latest_logret[product] = daily_log_returns_dt[-1]

    def update_performance(self, dt, weights):
        self.date = dt.date()
        ret = 0.0
        for product in self.products:
            if self.latest_logret[product][0] == self.date:
                logret_product = self.latest_logret[product][1]
            else:
                logret_product = 0.0
            ret = ret + weights[product]*(exp(logret_product) - 1)
        self.daily_log_returns = append(self.daily_log_returns, log(1 + ret))
        self.dates.append(self.date)
        self.current_drawdown = abs(current_dd(self.daily_log_returns))

    # Calculates the current drawdown i.e. the maximum drawdown with end point as the latest return value 
    def current_dd(self, returns):
        if returns.shape[0] < 2:
            return 0.0
        cum_returns = returns.cumsum()
        return -1.0*(max(cum_returns) - cum_returns[-1]) 

    # Calculates the global maximum drawdown i.e. the maximum drawdown till now
    def drawdown(self, returns):
        if returns.shape[0] < 2:
            return 0.0
        cum_returns = returns.cumsum()
        return -1.0*max(maximum.accumulate(cum_returns) - cum_returns) # Will return a negative value

    def rollsum(self, series, period):
        n = series.shape[0]
        if n < period:
            return array([]) #empty array
        return array([sum(series[i:i+period]) for i in xrange(0, n-period+1)]).astype(float)

    def mean_lowest_k_percent(self, series, k):
        sorted_series = sort(series)
        n = sorted_series.shape[0]
        _retval = 0
        if n <= 0 :
            _retval = 0
        else:
            _index_of_worst_k_percent = int((k/100.0)*n)
            if _index_of_worst_k_percent <= 0:
                _retval = sorted_series[0]
            else:
                _retval = mean(sorted_series[0:_index_of_worst_k_percent])
        return _retval

    # Prints the returns for k worst and k best days
    def print_extreme_days(self, k):
        _dates_returns = zip(self.dates, self.daily_log_returns)
        _sorted_returns = sorted(_dates_returns, key=lambda x: x[1]) # Sort by returns
        _end_index_worst_days = min(len(_sorted_returns), k)
        _start_index_best_days = max(0, len(_sorted_returns) - k)
        if len(_sorted_returns) > 0:
            _worst_days = _sorted_returns[0:_end_index_worst_days]
            _best_days = _sorted_returns[_start_index_best_days:len(_sorted_returns)]
            print '\nWorst %d Days:'%k
            for item in _worst_days:
                print item[0], ' : ', (exp(item[1])-1)*100.0, '%'
            print '\nBest %d Days:'%k
            for item in reversed(_best_days):
                print item[0], ' : ', (exp(item[1])-1)*100.0, '%'

    # non public function to save results to a file
    def _save_results(self):
        with open(self.returns_file, 'wb') as f:
            pickle.dump(zip(self.dates,self.daily_log_returns), f)

    def show_results(self):
        self.PnL = sum(self.PnLvector) # final sum of pnl of all trading days
        self.net_returns = (self.PnL*100.0)/self.initial_capital # final sum of pnl / initial capital
        self.annualized_PnL = 252.0 * mean(self.PnLvector)
        self.annualized_stdev_PnL = sqrt(252.0) * std(self.PnLvector)
        self.daily_returns = self.PnLvector * 100.0/self.value[0:self.value.shape[0] - 1]
        monthly_log_returns = self.rollsum(self.daily_log_returns, 21)
        quarterly_log_returns = self.rollsum(self.daily_log_returns, 63)
        yearly_log_returns = self.rollsum(self.daily_log_returns, 252)
        self._monthly_nominal_returns_percent = (exp(monthly_log_returns) - 1) * 100
        self._quarterly_nominal_returns_percent = (exp(quarterly_log_returns) - 1) * 100
        self._yearly_nominal_returns_percent = (exp(yearly_log_returns) - 1) * 100
        self.dml = (exp(self.mean_lowest_k_percent(self.daily_log_returns, 10)) - 1)*100.0
        self.mml = (exp(self.mean_lowest_k_percent(monthly_log_returns, 10)) - 1)*100.0
        self._worst_10pc_quarterly_returns = (exp(self.mean_lowest_k_percent(quarterly_log_returns, 10)) - 1) * 100.0
        self._worst_10pc_yearly_returns = (exp(self.mean_lowest_k_percent(yearly_log_returns, 10)) - 1) * 100.0
        self._annualized_returns_percent = (exp(252.0 * mean(self.daily_log_returns)) - 1) * 100.0
        self.annualized_stddev_returns = (exp(sqrt(252.0) * std(self.daily_log_returns)) - 1) * 100.0
        self.sharpe = self._annualized_returns_percent/self.annualized_stddev_returns
        self.skewness = ss.skew(self.daily_log_returns)
        self.kurtosis = ss.kurtosis(self.daily_log_returns)
        max_dd_log = self.drawdown(self.daily_log_returns)
        self.max_drawdown_percent = abs((exp(max_dd_log) - 1) * 100)
        self.max_drawdown_dollar = abs(self.drawdown(self.PnLvector))
        self.return_by_maxdrawdown = self._annualized_returns_percent/self.max_drawdown_percent
        self._annualized_pnl_by_max_drawdown_dollar = self.annualized_PnL/self.max_drawdown_dollar
        self.print_extreme_days(10)
        self._save_results()

        print "\nInitial Capital = %.10f\nNet PNL = %.10f \nTrading Cost = %.10f\nNet Returns = %.10f%%\nAnnualized PNL = %.10f\nAnnualized_Std_PnL = %.10f\nAnnualized_Returns = %.10f%% \nAnnualized_Std_Returns = %.10f%% \nSharpe Ratio = %.10f \nSkewness = %.10f\nKurtosis = %.10f\nDML = %.10f%%\nMML = %.10f%%\nQML = %.10f%%\nYML = %.10f%%\nMax Drawdown = %.10f%% \nMax Drawdown Dollar = %.10f \nAnnualized PNL by drawdown = %.10f \nReturn_drawdown_Ratio = %.10f \n" % (self.initial_capital, self.PnL, self.net_returns, self.annualized_PnL, self.annualized_stdev_PnL, self._annualized_returns_percent, self.annualized_stddev_returns, self.sharpe, self.skewness, self.kurtosis, self.dml, self.mml, self._worst_10pc_quarterly_returns, self._worst_10pc_yearly_returns, self.max_drawdown_percent, self.max_drawdown_dollar, self._annualized_pnl_by_max_drawdown_dollar, self.return_by_maxdrawdown)
