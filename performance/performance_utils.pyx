import numpy
import math
import datetime
import itertools
import scipy.stats as ss
from utils.calculate import convert_daily_returns_to_yyyymm_monthly_returns_pair
from utils.regular import is_float_zero

def annualized_returns(daily_log_returns):
    if daily_log_returns.shape[0] < 1:
        annualized_returns = 1.0
    else:
        annualized_returns = (math.exp(numpy.mean(daily_log_returns) * 252.0) - 1) * 100
        annualized_returns = max(1.0, min(50.0, annualized_returns)) # TODO check min max
    return annualized_returns

def annualized_stdev(daily_log_returns):
    if daily_log_returns.shape[0] < 2:
        annualized_stdev = 100.0
    else:
        annualized_stdev = (math.exp(math.sqrt(252.0) * numpy.std(daily_log_returns)) - 1) * 100.0
        annualized_stdev = max(1.0, min(100.0, annualized_stdev))
    return annualized_stdev   

def drawdown(returns):
    """Calculates the global maximum drawdown i.e. the maximum drawdown till now"""
    if returns.shape[0] < 2:
        return 0.0
    cum_returns = returns.cumsum()
    return -1.0*max(numpy.maximum.accumulate(cum_returns) - cum_returns) # Will return a negative value

def current_dd(max_cum_return, cum_returns):
    """Calculates the current drawdown i.e. the maximum drawdown with end point as the latest return value"""
    if cum_returns.shape[0] < 2:
        return 0.0
    return -1.0*(max_cum_return - cum_returns[-1])

def rollsum(series, period):
    n = series.shape[0]
    _ret = numpy.array([])
    if n >= period:
        _cur_sum = sum(series[0:period])
        _ret = numpy.append(_ret, _cur_sum)
        for i in range(1, n-period+1):
            _cur_sum = _cur_sum - series[i-1] + series[i+period-1]
            _ret = numpy.append(_ret, _cur_sum)
    return _ret

def compute_sortino(returns):
    """A modification of the Sharpe ratio that only takes negative deviation from a target return into consideration.
    The Sortino ratio subtracts the risk-free rate of return from the portfolios return, and then divides that by the downside deviation.
    A large Sortino ratio indicates there is a low probability of a large loss.
    Target return and risk free rate are taken as 0 in calculating sortino ratio in our implementation.

    Args:
        returns: daily log returns
    Returns:
        Sortino ratio for the given daily log returns series
    """
    neg_ret_sq = numpy.where(returns < 0, returns**2, 0) # Take square of all -ve returns, 0 for +ve returns.
    down_risk = math.sqrt(numpy.mean(neg_ret_sq)) # Downside risk as deviation from 0 in the negative
    sortino = 0
    if not is_float_zero(down_risk):
        sortino = (math.exp(252.0 * numpy.mean(returns)) - 1) / (math.exp(math.sqrt(252.0) * down_risk) -1)
    return sortino

def compute_yearly_sharpe(dates, returns):
    yyyy = [ date.strftime("%Y") for date in dates]
    yyyy_returns = zip(yyyy, returns)
    yearly_sharpe = []
    for key, rows in itertools.groupby(yyyy_returns, lambda x : x[0]):
        _returns = numpy.array([x[1] for x in rows])
        _ann_returns = annualized_returns(_returns)
        _ann_std = annualized_stdev(_returns)
        yearly_sharpe.append((key, _ann_returns/_ann_std ))
    return yearly_sharpe

def compute_losing_month_streak(dates, returns):
    monthly_returns = convert_daily_returns_to_yyyymm_monthly_returns_pair(dates, returns)
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
        percent_returns_in_streak = (math.exp(sum(x[1] for x in monthly_returns[global_start_idx : global_end_idx +1])) - 1)*100.0
        return (global_max_length, percent_returns_in_streak, monthly_returns[global_start_idx][0], monthly_returns[global_end_idx][0])

def compute_max_num_days_no_new_high(dates, PnLvector):
    """This function returns the maximum number of days the strategy did not make a new high"""
    if PnLvector.shape[0] < 1:
        return (0.0, '', '')
    current_num_days_no_new_high = 0
    current_start_idx = -1
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

def get_extreme_weeks(_dates, _returns, k):
    _extreme_weeks = ''
    _dated_weekly_returns = zip(_dates[0:len(_dates)-4], rollsum(_returns, 5))
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
        _extreme_weeks += 'Worst %d weeks =   '%k
        while _num_worst_weeks < k and _worst_week_idx < n:
            if (not _worst_start_dates_used) or _date_not_used(_sorted_returns[_worst_week_idx][0], _worst_start_dates_used, 5):
                _num_worst_weeks += 1
                _return = (math.exp(_sorted_returns[_worst_week_idx][1]) - 1)*100.0
                _extreme_weeks += str(_sorted_returns[_worst_week_idx][0]) + (' : %0.2f%%   ' % _return)
                _worst_start_dates_used.append(_sorted_returns[_worst_week_idx][0])
            _worst_week_idx += 1
        _extreme_weeks += '\nBest %d weeks =   '%k
        while _num_best_weeks < k and _best_week_idx >= 0:
            if (not _worst_start_dates_used) or _date_not_used(_sorted_returns[_best_week_idx][0], _best_start_dates_used, 5):
                _num_best_weeks += 1
                _return = (math.exp(_sorted_returns[_best_week_idx][1]) - 1)*100.0
                _extreme_weeks += str(_sorted_returns[_best_week_idx][0]) + (' : %0.2f%%   ' % _return)
                _best_start_dates_used.append(_sorted_returns[_best_week_idx][0])
            _best_week_idx -= 1
    return _extreme_weeks+'\n'

# Prints the returns for k worst and k best days
def get_extreme_days(dates, returns, k):
    _extreme_days = ''
    _dates_returns = zip(dates, returns)
    _sorted_returns = sorted(_dates_returns, key=lambda x: x[1]) # Sort by returns
    n = len(_sorted_returns)
    _num_worst_days = 0
    _worst_day_idx = 0
    _num_best_days = 0
    _best_day_idx = n-1
    if n > 0:
        _extreme_days += 'Worst %d days =   '%k
        while _num_worst_days < k and _worst_day_idx < n:
            _num_worst_days += 1
            _return = (math.exp(_sorted_returns[_worst_day_idx][1])-1)*100.0
            _extreme_days += str(_sorted_returns[_worst_day_idx][0]) + (' : %0.2f%%   ' % _return)
            _worst_day_idx += 1
        _extreme_days += '\nBest %d days =   '%k
        while _num_best_days < k and _best_day_idx >= 0:
            _num_best_days += 1
            _return = (math.exp(_sorted_returns[_best_day_idx][1])-1)*100.0
            _extreme_days += str(_sorted_returns[_best_day_idx][0]) + (' : %0.2f%%   ' % _return)
            _best_day_idx -= 1
    return _extreme_days + '\n'

def turnover(dates, amount_long_transacted, amount_short_transacted, portfolio_values):
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
                turnover_sum += (252.0/num_days_in_year)*min(amount_long_transacted_this_year, amount_short_transacted_this_year)/portfolio_values[i+1] # Size of value array is 1 more than number of tradable days
            else:
                turnover_sum += min(amount_long_transacted_this_year, amount_short_transacted_this_year)/portfolio_values[i+1]
            turnover_years_count += 1.0
            amount_long_transacted_this_year = 0.0
            amount_short_transacted_this_year = 0.0
            num_days_in_year = 0.0
    return turnover_sum*100/turnover_years_count

def mean_lowest_k_percent(series, k):
    sorted_series = numpy.sort(series)
    n = sorted_series.shape[0]
    _retval = 0
    if n <= 0 :
        _retval = 0
    else:
        _index_of_worst_k_percent = int((k/100.0)*n)
        if _index_of_worst_k_percent <= 0:
            _retval = sorted_series[0]
        else:
            _retval = numpy.mean(sorted_series[0:_index_of_worst_k_percent])
    return _retval

def drawdown_period_and_recovery_period(dates, _cum_returns):
    if _cum_returns.shape[0] < 2:
        _epoch = datetime.datetime.fromtimestamp(0).date()
        return ((_epoch, _epoch), (_epoch, _epoch))
    _end_idx_max_drawdown = numpy.argmax(numpy.maximum.accumulate(_cum_returns) - _cum_returns) # end of the period
    _start_idx_max_drawdown = numpy.argmax(_cum_returns[:_end_idx_max_drawdown+1]) # start of period
    _recovery_idx = -1
    _peak_value = _cum_returns[_start_idx_max_drawdown]
    _candidate_idx = numpy.argmax(_cum_returns[_end_idx_max_drawdown:] >= _peak_value) + _end_idx_max_drawdown
    if _cum_returns[_candidate_idx] >= _peak_value:
        _recovery_idx = _candidate_idx
    drawdown_period = (dates[_start_idx_max_drawdown], dates[_end_idx_max_drawdown])
    if _recovery_idx != -1:
        recovery_period = (dates[_end_idx_max_drawdown], dates[_recovery_idx])
    else:
        recovery_period = (dates[_end_idx_max_drawdown], datetime.date(2050,1,1)) # Never recovered
    return (drawdown_period, recovery_period)

def get_all_stats(dates, daily_log_returns):
    net_returns = (math.exp(numpy.sum(daily_log_returns)) - 1) * 100.0 
    monthly_log_returns = rollsum(daily_log_returns, 21)
    quarterly_log_returns = rollsum(daily_log_returns, 63)
    yearly_log_returns = rollsum(daily_log_returns, 252)
    worst_10pc_daily_returns = (math.exp(mean_lowest_k_percent(daily_log_returns, 10)) - 1)*100.0
    worst_10pc_monthly_returns = (math.exp(mean_lowest_k_percent(monthly_log_returns, 10)) - 1)*100.0
    worst_10pc_quarterly_returns = (math.exp(mean_lowest_k_percent(quarterly_log_returns, 10)) - 1) * 100.0
    worst_10pc_yearly_returns = (math.exp(mean_lowest_k_percent(yearly_log_returns, 10)) - 1) * 100.0
    annualized_returns_percent = annualized_returns(daily_log_returns)
    annualized_stddev_returns = annualized_stdev(daily_log_returns)
    sharpe = annualized_returns_percent/annualized_stddev_returns
    yearly_sharpe = compute_yearly_sharpe(dates, daily_log_returns)
    sortino = compute_sortino(daily_log_returns)
    _format_strings = ','.join([' %s : %0.2f'] * len(yearly_sharpe))
    _yearly_sharpe_tuple = tuple(list(itertools.chain(*yearly_sharpe)))
    _print_yearly_sharpe = _format_strings%_yearly_sharpe_tuple
    skewness = ss.skew(daily_log_returns)
    kurtosis = ss.kurtosis(daily_log_returns)
    max_drawdown_percent = abs((math.exp(drawdown(daily_log_returns)) - 1) * 100)
    drawdown_period, recovery_period = drawdown_period_and_recovery_period(dates, daily_log_returns.cumsum())
    return_by_maxdrawdown = annualized_returns_percent/max_drawdown_percent
    ret_var10 = abs(annualized_returns_percent/worst_10pc_daily_returns)
    #turnover_percent = turnover(self.dates, self.amount_long_transacted, self.amount_short_transacted, self.value)
    hit_loss_ratio = numpy.sum(numpy.where(daily_log_returns > 0, 1.0, 0.0))/numpy.sum(numpy.where(daily_log_returns < 0, 1.0, 0.0))
    gain_pain_ratio = numpy.sum(daily_log_returns)/numpy.sum(numpy.where(daily_log_returns < 0, -daily_log_returns, 0.0))
    max_num_days_no_new_high = compute_max_num_days_no_new_high(dates, daily_log_returns)
    losing_month_streak = compute_losing_month_streak(dates, daily_log_returns)
    extreme_days = get_extreme_days(dates, daily_log_returns, 5)
    extreme_weeks = get_extreme_weeks(dates, daily_log_returns, 5)
    #if len(self.leverage) > 0:
    #        _leverage_params = (min(self.leverage), max(self.leverage), numpy.mean(self.leverage), numpy.std(self.leverage))
    #    else:
    #        _leverage_params = (0, 0, 0, 0)
    _stats = extreme_days + extreme_weeks 
    _stats += ("\nNet Returns = %.2f%%\nAnnualized_Returns = %.2f%% \nAnnualized_Std_Returns = %.2f%% \nSharpe Ratio = %.2f \nSortino Ratio = %.2f\nSkewness = %.2f\nKurtosis = %.2f\nDML = %.2f%%\nMML = %.2f%%\nQML = %.2f%%\nYML = %.2f%%\nMax Drawdown = %.2f%% \nDrawdown Period = %s to %s\nDrawdown Recovery Period = %s to %s\nReturn_drawdown_Ratio = %.2f\nReturn Var10 ratio = %.2f\nYearly_sharpe = " + _print_yearly_sharpe + "\nHit Loss Ratio = %0.2f\nGain Pain Ratio = %0.2f\nMax num days with no new high = %d from %s to %s\nLosing month streak = Lost %0.2f%% in %d months from %s to %s\n") % (net_returns, annualized_returns_percent, annualized_stddev_returns, sharpe, sortino, skewness, kurtosis, worst_10pc_daily_returns, worst_10pc_monthly_returns, worst_10pc_quarterly_returns, worst_10pc_yearly_returns, max_drawdown_percent, drawdown_period[0], drawdown_period[1], recovery_period[0], recovery_period[1], return_by_maxdrawdown, ret_var10, hit_loss_ratio, gain_pain_ratio, max_num_days_no_new_high[0], max_num_days_no_new_high[1], max_num_days_no_new_high[2], losing_month_streak[1], losing_month_streak[0], losing_month_streak[2], losing_month_streak[3])
    return _stats 
