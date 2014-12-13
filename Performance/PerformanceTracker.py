import sys
import os
import datetime
from numpy import *
import scipy.stats as ss
import pickle
import itertools

from BackTester.BackTester_Listeners import BackTesterListener
from BackTester.BackTester import BackTester
from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import EndOfDayListener
from Utils.Regular import check_eod, get_dt_from_date, get_next_futures_contract, is_future
from Utils.Calculate import find_most_recent_price, find_most_recent_price_future, get_current_notional_amounts, convert_daily_to_monthly_returns
from Utils.benchmark_comparison import get_monthly_correlation_to_benchmark
from Utils import defaults
from BookBuilder.BookBuilder import BookBuilder
from Utils.global_variables import Globals

# TODO {gchak} PerformanceTracker is probably a class that just pertains to the performance of one strategy
# We need to change it from listening to executions from BackTeser, to being called on from the OrderManager,
# which will in turn listen to executions from the BackTester

'''Performance tracker listens to the Dispatcher for concurrent events so that it can record the daily returns
 It also listens to the Backtester for any new filled orders
 At the end it shows the results and plot the cumulative PnL graph
 It outputs the list of [dates,dailyreturns] to returns_file for later analysis
 It outputs the portfolio snapshots and orders in the positions_file for debugging
'''
class PerformanceTracker(BackTesterListener, EndOfDayListener):

    def __init__(self, products, _startdate, _enddate, _config, _log_filename):
        self.date = get_dt_from_date(_startdate).date()  #The earliest date for which daily stats still need to be computed
        if _config.has_option('Parameters', 'debug_level'):
            self.debug_level = _config.getint('Parameters','debug_level')
        else:
            self.debug_level = defaults.DEBUG_LEVEL  # Default value of debug level,in case not specified in config file
        if self.debug_level > 0:
            self.positions_file = 'logs/'+_log_filename+'/positions.txt'
        if self.debug_level > 2:
            self.amount_transacted_file = open('logs/'+_log_filename+'/amount_transacted.txt', 'w')
            self.amount_transacted_file.write('date,amount_transacted\n')
        self.returns_file = 'logs/'+_log_filename+'/returns.txt'
        self.stats_file = 'logs/'+_log_filename+'/stats.txt'
        self.products = products
        self.conversion_factor = Globals.conversion_factor
        self.currency_factor = Globals.currency_factor
        self.num_shares_traded = dict([(product, 0) for product in self.products])
        self.benchmarks = ['VBLTX', 'VTSMX']
        if _config.has_option('Benchmarks', 'products'):
            self.benchmarks.extend(_config.get('Benchmarks','products').split(','))
        self.dates = []
        self.PnL = 0
        self.net_returns = 0
        self.initial_capital = _config.getfloat('Parameters', 'initial_capital')
        self.value = array([self.initial_capital])  # Track end of day values of the portfolio
        self.PnLvector = empty(shape=(0))
        self.annualized_PnL = 0
        self.annualized_stdev_PnL = 0
        self._annualized_returns_percent = 0
        self.annualized_stddev_returns = 0
        self.sharpe = 0
        self.yearly_sharpe = []
        self.daily_returns = empty(shape=(0))
        self.daily_log_returns = empty(shape=(0))
        self.net_log_return = 0
        self._monthly_nominal_returns_percent = empty(shape=(0))
        self._quarterly_nominal_returns_percent = empty(shape=(0))
        self._yearly_nominal_returns_percent = empty(shape=(0))
        self.dml = 0
        self.mml = 0
        self._worst_10pc_quarterly_returns = 0
        self._worst_10pc_yearly_returns = 0
        self.current_loss = 0
        self.current_drawdown = 0
        self.current_year_trading_cost = [datetime.datetime.fromtimestamp(0).date().year, 0.0]
        self.max_drawdown_percent = 0
        self.drawdown_period = (datetime.datetime.fromtimestamp(0).date(), datetime.datetime.fromtimestamp(0).date())
        self.recovery_period = (datetime.datetime.fromtimestamp(0).date(), datetime.datetime.fromtimestamp(0).date())
        self.max_drawdown_dollar = 0
        self.return_by_maxdrawdown = 0
        self._annualized_pnl_by_max_drawdown_dollar = 0
        self.skewness = 0
        self.kurtosis = 0
        self.trading_cost = 0
        self.total_orders = 0
        self.todays_amount_transacted = 0.0
        self.todays_long_amount_transacted = 0.0
        self.todays_short_amount_transacted = 0.0
        self.amount_long_transacted = []
        self.amount_short_transacted = []
        self.turnover_percent = 0.0
        self.total_amount_transacted = 0.0
        self.hit_loss_ratio = 0.0
        self.gain_pain_ratio = 0.0
        self.max_num_days_no_new_high = (0, '', '')
        self.leverage = empty(shape=(0))
        self.leverage_stats = (0.0, 0.0, 0.0, 0.0) # (min, max, average, stddev)
        self.losing_months_streak = (0, 0.0, '', '')
        self.correlation_to_spx = 0.0
        self.correlation_to_agg = 0.0

        # Listens to end of day combined event to be able to compute the market ovement based effect on PNL
        Dispatcher.get_unique_instance(products, _startdate, _enddate, _config).add_end_of_day_listener(self)
        self.bb_objects = {}
        for product in products:
            BackTester.get_unique_instance(product, _startdate, _enddate, _config).add_listener(self) # Listen to Backtester for filled orders
            self.bb_objects[product] = BookBuilder.get_unique_instance(product, _startdate, _enddate, _config)

    def on_order_update(self, filled_orders, dt):
        for order in filled_orders:
            self.portfolio.cash -= order['cost']
            self.num_shares_traded[order['product']] = self.num_shares_traded[order['product']] + abs(order['amount'])
            self.trading_cost = self.trading_cost + order['cost']
            if dt.date().year > self.current_year_trading_cost[0]:
                self.current_year_trading_cost[1] = order['cost']
                self.current_year_trading_cost[0] = dt.date().year
            else:
                self.current_year_trading_cost[1] += order['cost']
            self.total_orders = self.total_orders + 1
            if order['type'] == 'normal': # Aggressive order not accounted for
                self.todays_amount_transacted += abs(order['value'])
                self.total_amount_transacted += abs(order['value'])
                if order['value'] > 0:
                    self.todays_long_amount_transacted += abs(order['value'])
                else:
                    self.todays_short_amount_transacted += abs(order['value'])

    # Called by Dispatcher
    def on_end_of_day(self, date):
        self.compute_daily_stats(date)
        _current_dd_log = self.current_dd(self.daily_log_returns)
        self.current_drawdown = abs((exp(_current_dd_log) - 1)* 100)
        self.current_loss = abs(min(0.0, (exp(self.net_log_return) - 1)*100.0))
        if self.debug_level > 0:
            self.print_snapshot(date)

    # Computes the portfolio value at ENDOFDAY on 'date'
    def get_portfolio_value(self, date):
        netValue = self.portfolio.cash
        for product in self.products:
            if self.portfolio.num_shares[product] != 0:
                if is_future(product):
                    current_price = find_most_recent_price_future(self.bb_objects[product].dailybook, self.bb_objects[get_next_futures_contract(product)].dailybook, date)
                else:
                    current_price = find_most_recent_price(self.bb_objects[product].dailybook, date)
                netValue = netValue + current_price * self.portfolio.num_shares[product] * self.conversion_factor[product] * self.currency_factor[product][date]
        return netValue

    # Computes the daily stats for the most recent trading day prior to 'date'
    # TOASK {gchak} Do we ever expect to run this function without current date ?
    def compute_daily_stats(self, date):
        self.date = date
        if self.total_orders > 0: # If no orders have been filled,it implies trading has not started yet
            todaysValue = self.get_portfolio_value(self.date)
            self.value = append(self.value, todaysValue)
            self.PnLvector = append(self.PnLvector, (self.value[-1] - self.value[-2]))  # daily PnL = Value of portfolio on last day - Value of portfolio on 2nd last day
            if self.value[-1] <= 0:
                _logret_today = -1000 # real value is -inf
            else:
                _logret_today = log(self.value[-1]/self.value[-2])
            self.daily_log_returns = append(self.daily_log_returns, _logret_today)
            self.net_log_return += self.daily_log_returns[-1]
            self.amount_long_transacted.append(self.todays_long_amount_transacted)
            self.amount_short_transacted.append(self.todays_short_amount_transacted)
            if self.debug_level > 2:
                self.print_transacted_amount(self.todays_amount_transacted)
            self.todays_amount_transacted = 0.0
            self.todays_long_amount_transacted = 0.0
            self.todays_short_amount_transacted = 0.0
            _leverage = (abs(min(0.0, self.portfolio.cash)) + self.value[-1])/(max(0.0, self.portfolio.cash)+ self.value[-1])
            self.leverage = append(self.leverage, _leverage)
            self.dates.append(self.date)

    def print_transacted_amount(self, amount):
        s = str(self.date) + ',%f'% (amount)
        self.amount_transacted_file.write(s + '\n')

    def print_filled_orders(self, filled_orders):
        if len(filled_orders) == 0: return
        s = 'ORDER FILLED : '
        for order in filled_orders:
            s = s + 'id: %d  product: %s  amount: %f  cost: %f  value: %f  fill_price: %f'%(order['id'], order['product'], order['amount'], order['cost'], order['value'], order['fill_price'])
        text_file = open(self.positions_file, "a")
        text_file.write("%s\n" % s)
        text_file.close()

    def print_snapshot(self, date):
        text_file = open(self.positions_file, "a")
        if self.PnLvector.shape[0] > 0:
            s = "\nPortfolio snapshot at EndOfDay %s\nPnL for today: %f\nPortfolio Value:%f\nCash:%f\nPositions:%s\n" % (date, self.PnLvector[-1], self.value[-1], self.portfolio.cash, str(self.portfolio.num_shares))
        else:      
            s = "\nPortfolio snapshot at EndOfDay %s\nPnL for today: Trading has not started\nPortfolio Value:%f\nCash:%f\nPositions:%s\n" % (date, self.value[-1], self.portfolio.cash, str(self.portfolio.num_shares))
        (notional_amounts, net_value) = get_current_notional_amounts(self.bb_objects, self.portfolio, self.conversion_factor, self.currency_factor, date)
        s = s + 'Money Allocation: %s\n\n' % notional_amounts
        text_file.write(s)
        text_file.close()

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

    def drawdown_period_and_recovery_period(self, dates, returns):
        if returns.shape[0] < 2:
            _epoch = datetime.datetime.fromtimestamp(0).date()
            return ((_epoch, _epoch), (_epoch, _epoch))
        _cum_returns = returns.cumsum()
        _end_idx_max_drawdown = argmax(maximum.accumulate(_cum_returns) - _cum_returns) # end of the period
        _start_idx_max_drawdown = argmax(_cum_returns[:_end_idx_max_drawdown]) # start of period
        _recovery_idx = -1
        _peak_value = _cum_returns[_start_idx_max_drawdown]
        _candidate_idx = argmax(_cum_returns[_end_idx_max_drawdown:] >= _peak_value) + _end_idx_max_drawdown
        if _cum_returns[_candidate_idx] >= _peak_value:
            _recovery_idx = _candidate_idx
        drawdown_period = (dates[_start_idx_max_drawdown], dates[_end_idx_max_drawdown])
        if _recovery_idx != -1:
            recovery_period = (dates[_end_idx_max_drawdown], dates[_recovery_idx])
        else:
            recovery_period = (dates[_end_idx_max_drawdown], datetime.date(2050,1,1)) # Never recovered
        return (drawdown_period, recovery_period)

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

    def turnover(self, dates, amount_long_transacted, amount_short_transacted):
        if len(dates) < 1:
            return 0.0
        turnover_sum = 0.0
        turnover_years_count = 0.0
        amount_long_transacted_this_year = 0.0
        amount_short_transacted_this_year = 0.0
        num_days_in_year = 0.0
        for i in range(min(len(dates)-1, 5), len(dates)): # Initial buying not to be considered as turnover
            amount_long_transacted_this_year += amount_long_transacted[i]
            amount_short_transacted_this_year += amount_short_transacted[i]
            num_days_in_year += 1.0
            if i == len(dates)-1 or dates[i+1].year != dates[i].year:
                if num_days_in_year < 252:
                    turnover_sum += (252.0/num_days_in_year)*min(amount_long_transacted_this_year, amount_short_transacted_this_year)/self.value[i+1] # Size of value array is 1 more than number of tradable days
                else:
                    turnover_sum += min(amount_long_transacted_this_year, amount_short_transacted_this_year)/self.value[i+1]
                turnover_years_count += 1.0
                amount_long_transacted_this_year = 0.0
                amount_short_transacted_this_year = 0.0
                num_days_in_year = 0.0
        return turnover_sum*100/turnover_years_count

    # Prints the returns for k worst and k best days
    def extreme_days(self, k):
        _extreme_days = ''
        _dates_returns = zip(self.dates, self.daily_log_returns)
        _sorted_returns = sorted(_dates_returns, key=lambda x: x[1]) # Sort by returns
        n = len(_sorted_returns)
        _num_worst_days = 0
        _worst_day_idx = 0
        _num_best_days = 0
        _best_day_idx = n-1
        if n > 0:
            _extreme_days += 'Worst %d days\n'%k
            while _num_worst_days < k and _worst_day_idx < n:
                _num_worst_days += 1
                _return = (exp(_sorted_returns[_worst_day_idx][1])-1)*100.0
                _extreme_days += str(_sorted_returns[_worst_day_idx][0]) + ' : ' + str(_return) + '\n'
                _worst_day_idx += 1
            _extreme_days += 'Best %d days\n'%k
            while _num_best_days < k and _best_day_idx >= 0:
                _num_best_days += 1
                _return = (exp(_sorted_returns[_best_day_idx][1])-1)*100.0
                _extreme_days += str(_sorted_returns[_best_day_idx][0]) + ' : ' + str(_return) + '\n'
                _best_day_idx -= 1     
        return _extreme_days

    def extreme_weeks(self, _dates, _returns, k):
        _extreme_weeks = ''
        _dated_weekly_returns = zip(_dates[0:len(_dates)-4], self.rollsum(_returns, 5))
        _sorted_returns = sorted(_dated_weekly_returns, key=lambda x: x[1]) # Sort by returns
        n = len(_sorted_returns)
        _num_worst_weeks = 0
        _worst_week_idx = 0
        _num_best_weeks = 0
        _best_week_idx = n-1
        _worst_start_dates_used = []
        _best_start_dates_used = []

        def _date_not_used(_date, _used_dates, interval = 5):
            _not_used = True
            for _used_date in _used_dates:
                if abs((_date - _used_date).days) < interval:
                    _not_used = False
                    break
            return _not_used    

        if n > 0:
            _extreme_weeks += 'Worst %d weeks\n'%k
            while _num_worst_weeks < k and _worst_week_idx < n:
                if (not _worst_start_dates_used) or _date_not_used(_sorted_returns[_worst_week_idx][0], _worst_start_dates_used, 5):
                    _num_worst_weeks += 1
                    _return = (exp(_sorted_returns[_worst_week_idx][1]) - 1)*100.0
                    _extreme_weeks += str(_sorted_returns[_worst_week_idx][0]) + ' : ' + str(_return) + '\n'
                    _worst_start_dates_used.append(_sorted_returns[_worst_week_idx][0])
                _worst_week_idx += 1
            _extreme_weeks += 'Best %d weeks\n'%k
            while _num_best_weeks < k and _best_week_idx >= 0:
                if (not _worst_start_dates_used) or _date_not_used(_sorted_returns[_best_week_idx][0], _best_start_dates_used, 5):
                    _num_best_weeks += 1
                    _return = (exp(_sorted_returns[_best_week_idx][1]) - 1)*100.0
                    _extreme_weeks += str(_sorted_returns[_best_week_idx][0]) + ' : ' + str(_return) + '\n'
                    _best_start_dates_used.append(_sorted_returns[_best_week_idx][0])
                _best_week_idx -= 1     
        return _extreme_weeks

    def compute_max_num_days_no_new_high(self, dates, PnLvector):
        """This function returns the maximum number of days the strategy did not make a new high"""
        if PnLvector.shape[0] < 1:
            return (0.0, '', '')
        current_num_days_no_new_high = 0
        current_start_idx = 0
        current_high = 0.0
        max_num_days_no_new_high = 0
        max_start_idx = -1
        max_end_idx = -1
        cum_PnL = PnLvector.cumsum()
        for i in xrange(len(cum_PnL)):
            if cum_PnL[i] >= current_high:
                #new high
                current_num_days_no_new_high = 0
                current_high = cum_PnL[i]
                current_start_idx = i
            else:
                current_num_days_no_new_high += 1
                if current_num_days_no_new_high > max_num_days_no_new_high:
                    max_num_days_no_new_high = current_num_days_no_new_high
                    max_start_idx = current_start_idx + 1
                    max_end_idx = i
        if max_start_idx == -1 or max_end_idx == -1:
            return (0.0, '', '')
        else:
            return (max_num_days_no_new_high, dates[max_start_idx], dates[max_end_idx])

    def compute_losing_month_streak(self, dates, returns):
        monthly_returns = convert_daily_to_monthly_returns(dates, returns)
        # Find max length -ve number subarray
        global_start_idx = -1
        global_end_idx = -1
        current_start_idx = 0
        current_end_idx = 0
        current_idx = 0
        global_max_length = 0
        while current_idx < len(monthly_returns):
            while current_idx < len(monthly_returns) and monthly_returns[current_idx][1] >= 0:
                current_idx += 1
            if current_idx < len(monthly_returns):
                current_start_idx = current_idx
                current_idx += 1
                while current_idx < len(monthly_returns) and monthly_returns[current_idx][1] < 0:
                    current_idx += 1
                current_end_idx = current_idx - 1
                if global_max_length < current_end_idx - current_start_idx + 1:
                    global_max_length = current_end_idx - current_start_idx + 1
                    global_start_idx = current_start_idx
                    global_end_idx = current_end_idx
                 
        if global_end_idx == -1 or global_start_idx == -1:
            return (0, 0, '', '')
        else:
            percent_returns_in_streak = (exp(sum(x[1] for x in monthly_returns[global_start_idx : global_end_idx +1])) - 1)*100.0
            return (global_max_length, percent_returns_in_streak, monthly_returns[global_start_idx][0], monthly_returns[global_end_idx][0])       

    def compute_yearly_sharpe(self, dates, returns):
        yyyy = [ date.strftime("%Y") for date in dates]
        yyyy_returns = zip(yyyy, returns)
        yearly_sharpe = []
        for key, rows in itertools.groupby(yyyy_returns, lambda x : x[0]):
            _returns = array([x[1] for x in rows])
            _ann_returns = (exp(252.0 * mean(_returns)) - 1) * 100.0
            _ann_std = (exp(sqrt(252.0) * std(_returns)) - 1) * 100.0
            yearly_sharpe.append((key, _ann_returns/_ann_std ))
        return yearly_sharpe

    # non public function to save results to a file
    def _save_results(self):
        with open(self.returns_file, 'wb') as f:
            pickle.dump(zip(self.dates,self.daily_log_returns), f)

    def _save_stats(self, _stats):
        text_file = open(self.stats_file, "w")
        text_file.write("%s" % (_stats))
        text_file.close()

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
        self.yearly_sharpe = self.compute_yearly_sharpe(self.dates, self.daily_log_returns)
        _format_strings = ','.join([' %s : %0.2f'] * len(self.yearly_sharpe))
        _yearly_sharpe_tuple = tuple(list(itertools.chain(*self.yearly_sharpe)))
        _print_yearly_sharpe = _format_strings%_yearly_sharpe_tuple
        self.skewness = ss.skew(self.daily_log_returns)
        self.kurtosis = ss.kurtosis(self.daily_log_returns)
        max_dd_log = self.drawdown(self.daily_log_returns)
        self.max_drawdown_percent = abs((exp(max_dd_log) - 1) * 100)
        self.drawdown_period, self.recovery_period = self.drawdown_period_and_recovery_period(self.dates, self.daily_log_returns)
        self.max_drawdown_dollar = abs(self.drawdown(self.PnLvector))
        self.return_by_maxdrawdown = self._annualized_returns_percent/self.max_drawdown_percent
        self._annualized_pnl_by_max_drawdown_dollar = self.annualized_PnL/self.max_drawdown_dollar
        self.ret_var10 = abs(self._annualized_returns_percent/self.dml)
        self.turnover_percent = self.turnover(self.dates, self.amount_long_transacted, self.amount_short_transacted)
        self.hit_loss_ratio = sum(where(self.daily_log_returns > 0, 1.0, 0.0))/sum(where(self.daily_log_returns < 0, 1.0, 0.0))
        self.gain_pain_ratio = sum(self.daily_log_returns)/sum(where(self.daily_log_returns < 0, -self.daily_log_returns, 0.0))
        self.max_num_days_no_new_high = self.compute_max_num_days_no_new_high(self.dates, self.PnLvector)
        self.losing_month_streak = self.compute_losing_month_streak(self.dates, self.daily_log_returns)
        _extreme_days = self.extreme_days(5)
        _extreme_weeks = self.extreme_weeks(self.dates, self.daily_log_returns, 5)
        if len(self.leverage) > 0:
            _leverage_params = (min(self.leverage), max(self.leverage), mean(self.leverage), std(self.leverage))
        else:
            _leverage_params = (0, 0, 0, 0)
        self._save_results()
        _stats = _extreme_days + _extreme_weeks 
        _stats += ("\nInitial Capital = %.2f\nNet PNL = %.2f \nTrading Cost = %.2f\nNet Returns = %.2f%%\nAnnualized PNL = %.2f\nAnnualized_Std_PnL = %.2f\nAnnualized_Returns = %.2f%% \nAnnualized_Std_Returns = %.2f%% \nSharpe Ratio = %.2f \nSkewness = %.2f\nKurtosis = %.2f\nDML = %.2f%%\nMML = %.2f%%\nQML = %.2f%%\nYML = %.2f%%\nMax Drawdown = %.2f%% \nDrawdown Period = %s to %s\nDrawdown Recovery Period = %s to %s\nMax Drawdown Dollar = %.2f \nAnnualized PNL by drawdown = %.2f \nReturn_drawdown_Ratio = %.2f\nReturn Var10 ratio = %.2f\nYearly_sharpe = " + _print_yearly_sharpe + "\nHit Loss Ratio = %0.2f\nGain Pain Ratio = %0.2f\nMax num days with no new high = %d from %s to %s\nLosing month streak = Lost %0.2f%% in %d months from %s to %s\nTurnover = %0.2f%%\nLeverage = Min : %0.2f, Max : %0.2f, Average : %0.2f, Stddev : %0.2f\nTrading Cost = %0.2f\nTotal Money Transacted = %0.2f\nTotal Orders Placed = %d\n") % (self.initial_capital, self.PnL, self.trading_cost, self.net_returns, self.annualized_PnL, self.annualized_stdev_PnL, self._annualized_returns_percent, self.annualized_stddev_returns, self.sharpe, self.skewness, self.kurtosis, self.dml, self.mml, self._worst_10pc_quarterly_returns, self._worst_10pc_yearly_returns, self.max_drawdown_percent, self.drawdown_period[0], self.drawdown_period[1], self.recovery_period[0], self.recovery_period[1], self.max_drawdown_dollar, self._annualized_pnl_by_max_drawdown_dollar, self.return_by_maxdrawdown, self.ret_var10, self.hit_loss_ratio, self.gain_pain_ratio, self.max_num_days_no_new_high[0], self.max_num_days_no_new_high[1], self.max_num_days_no_new_high[2], self.losing_month_streak[1], self.losing_month_streak[0], self.losing_month_streak[2], self.losing_month_streak[3], self.turnover_percent, _leverage_params[0], _leverage_params[1], _leverage_params[2], _leverage_params[3], self.trading_cost, self.total_amount_transacted, self.total_orders)
        for benchmark in self.benchmarks:
            _stats += 'Correlation to %s = %0.2f\n' % (benchmark, get_monthly_correlation_to_benchmark(self.dates, self.daily_log_returns, benchmark))
        print _stats
        self._save_stats(_stats)
