# cython: profile=True
import sys
import os
import datetime
import numpy
import math
import scipy.stats as ss
import marshal
import cPickle
import itertools
from collections import deque

from backtester.backtester_listeners import BackTesterListener
from backtester.backtester import BackTester
from dispatcher.dispatcher import Dispatcher
from dispatcher.dispatcher_listeners import EndOfDayListener, TaxPaymentDayListener, DistributionDayListener
from utils.regular import check_eod, get_dt_from_date, get_next_futures_contract, is_float_zero, is_future, shift_future_symbols, is_margin_product, dict_to_string, parse_results
from utils.calculate import find_most_recent_price, find_most_recent_price_future, get_current_notional_amounts
from utils.benchmark_comparison import get_benchmark_stats
from utils import defaults
from bookbuilder.bookbuilder import BookBuilder
from utils.global_variables import Globals
from utils.json_parser import JsonParser
from performance_utils import drawdown, current_dd, drawdown_period_and_recovery_period, rollsum, mean_lowest_k_percent, turnover, get_extreme_days, get_extreme_weeks, compute_max_num_days_no_new_high, compute_yearly_sharpe, compute_sortino, compute_losing_month_streak, annualized_returns, annualized_stdev

# TODO {gchak} PerformanceTracker is probably a class that just pertains to the performance of one strategy
# We need to change it from listening to executions from BackTester, to being called on from the OrderManager,
# which will in turn listen to executions from the BackTester

'''Performance tracker listens to the Dispatcher for concurrent events so that it can record the daily returns
 It also listens to the Backtester for any new filled orders
 It outputs the list of [dates,dailyreturns] to returns_file for later analysis
 It outputs the portfolio snapshots and orders in the positions_file for debugging
'''
class PerformanceTracker(BackTesterListener, EndOfDayListener, TaxPaymentDayListener, DistributionDayListener):

    def __init__(self, products, _startdate, _enddate, _config):
        self.products = products
        self.date = get_dt_from_date(_startdate).date()  #The earliest date for which daily stats still need to be computed
        self.conversion_factor = Globals.conversion_factor
        self.currency_factor = Globals.currency_factor
        self.product_to_currency = Globals.product_to_currency
        self.num_shares_traded = dict([(product, 0) for product in self.products])
        self.benchmarks = ['VBLTX', 'VTSMX', 'AQRIX','AQMIX', 'BTOP50', 'TULIP', 'AHLDIV']
        if _config.has_option('Benchmarks', 'products'):
            self.benchmarks.extend(_config.get('Benchmarks','products').split(','))
        self.dates = []
        self.PnL = 0
        self.todays_realized_pnl = dict([(_currency, 0) for _currency in self.currency_factor.keys()]) # Map from currency to todays realized pnl in the currency
        self.average_trade_price = dict([(_product, 0) for _product in self.products]) # Map from product to average trade price for the open trades in the product
        self.net_returns = 0
        self.initial_capital = _config.getfloat('Parameters', 'initial_capital')
        self.value = numpy.array([self.initial_capital])  # Track end of day values of the portfolio
        self.PnLvector = numpy.empty(shape=(0))
        self.annualized_PnL = 0
        self.annualized_stdev_PnL = 0
        self.annualized_returns_percent = 0
        self.annualized_stddev_returns = 0
        self.sharpe = 0
        self.yearly_sharpe = []
        self.sortino = 0
        self.yearly_sotino = []
        self.daily_log_returns = numpy.empty(shape=(0))
        self.cum_log_returns = numpy.empty(shape=(0))
        self.max_cum_log_return = -1000 # Read as -inf
        self.net_log_return = 0
        self.worst_10pc_daily_returns = 0
        self.worst_10pc_monthly_returns = 0
        self.worst_10pc_quarterly_returns = 0
        self.worst_10pc_yearly_returns = 0
        self.current_loss = 0
        self.current_drawdown = 0
        self.current_year_trading_cost = [datetime.datetime.fromtimestamp(0).date().year, 0.0]
        self.max_drawdown_percent = 0
        self.drawdown_period = (datetime.datetime.fromtimestamp(0).date(), datetime.datetime.fromtimestamp(0).date())
        self.recovery_period = (datetime.datetime.fromtimestamp(0).date(), datetime.datetime.fromtimestamp(0).date())
        self.max_drawdown_dollar = 0
        self.return_by_maxdrawdown = 0
        self.annualized_pnl_by_max_drawdown_dollar = 0
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
        self.leverage = numpy.empty(shape=(0))
        self.leverage_stats = (0.0, 0.0, 0.0, 0.0) # (min, max, average, stddev)
        self.losing_months_streak = (0, 0.0, '', '')
        self.correlation_to_spx = 0.0
        self.correlation_to_agg = 0.0

        # For tax adjusted returns
        self.short_term_tax_rate = .396 #39.6%
        self.long_term_tax_rate = .196 #19.6%
        self.dividend_tax_rate = 0.4 #40%
        if _config.has_option('Parameters', 'short_term_tax_rate'):
            self.short_term_tax_rate = _config.getfloat('Parameters', 'short_term_tax_rate')/100.0
        if _config.has_option('Parameters', 'long_term_tax_rate'):
            self.long_term_tax_rate = _config.getfloat('Parameters', 'long_term_tax_rate')/100.0
        if _config.has_option('Parameters', 'dividend_tax_rate'):
            self.dividend_tax_rate = _config.getfloat('Parameters', 'dividend_tax_rate')/100.0
        self.short_term_tax_liability_realized = 0.0
        self.short_term_tax_liability_unrealized = 0.0
        self.long_term_tax_liability_realized = 0.0
        self.long_term_tax_liability_unrealized = 0.0
        self.long_orders = {}
        self.product_type = Globals.product_type
        for product in self.products:
            if self.product_type[product] in ['etf', 'fund', 'stock']:
                self.long_orders[product] = deque()
                
        # Listens to end of day combined event to be able to compute the market ovement based effect on PNL
        _dispatcher = Dispatcher.get_unique_instance(products, _startdate, _enddate, _config)
        _dispatcher.add_end_of_day_listener(self)
        _dispatcher.add_tax_payment_day_listener(self)
        _dispatcher.add_distribution_day_listener(self)
        self.bb_objects = {}
        for product in products:
            BackTester.get_unique_instance(product, _startdate, _enddate, _config).add_listener(self) # Listen to Backtester for filled orders
            self.bb_objects[product] = BookBuilder.get_unique_instance(product, _startdate, _enddate, _config)

    def on_last_trading_day(self, _base_symbol, future_mappings):
        shift_future_symbols(self.average_trade_price, future_mappings)

    def on_order_update(self, filled_orders, dt):
        for order in filled_orders:
            _product = order['product']
            if self.product_type[_product] in ['etf', 'fund', 'stock']: # Assuming long only portfolios for these product types to calculate tax adjusted returns
                if order['amount'] > 0:
                    self.long_orders[_product].append((order['dt'], order['fill_price'], order['amount']))    
                else:
                    current_short_amount = - order['amount']
                    while current_short_amount > 0:
                        matched_order = self.long_orders[_product][0]  # Find long order in FIFO queue that has not been matched till now
                        if matched_order[2] > current_short_amount:  # Look for no more matches, long order amount > short order amount
                            _closed_amount = current_short_amount
                        else:
                            _closed_amount = matched_order[2]        
                            self.long_orders[_product].popleft()  # Remove the long order from queue
                        current_short_amount -= _closed_amount  # Update remaining short amount
                        _profit = _closed_amount * (order['fill_price'] - matched_order[1])  
                        time_diff = order['dt'] - matched_order[0]  # Look at time dif to decide short or long term
                        time_diff_in_years = (time_diff.days + time_diff.seconds/86400.0)/365.2425
                        if time_diff_in_years < 1.0: # Short term gain
                            self.short_term_tax_liability_realized += _profit
                        else: # Long Term gain
                            self.long_term_tax_liability_realized += _profit
            self.update_average_trade_price_and_portfolio(order)
            self.num_shares_traded[order['product']] = self.num_shares_traded[order['product']] + abs(order['amount'])
            self.trading_cost = self.trading_cost + order['cost']
            if dt.date().year > self.current_year_trading_cost[0]:
                self.current_year_trading_cost[1] = order['cost']
                self.current_year_trading_cost[0] = dt.date().year
            else:
                self.current_year_trading_cost[1] += order['cost']
            self.total_orders = self.total_orders + 1
            if order['type'] == 'normal': # Aggressive orders not accounted for due to rollover
                self.todays_amount_transacted += abs(order['value'])
                self.total_amount_transacted += abs(order['value'])
                if order['value'] > 0:
                    self.todays_long_amount_transacted += abs(order['value'])
                else:
                    self.todays_short_amount_transacted += abs(order['value'])

    def update_average_trade_price_and_portfolio(self, order):
        _product = order['product']
        _current_num_contracts = self.portfolio.num_shares[_product]
        if is_margin_product(_product): # If we are required to post margin for the product
            _currency = self.product_to_currency[_product]
            _total_num_contracts = order['amount'] + _current_num_contracts
            _current_price = self.bb_objects[_product].dailybook[-1][1]
            _direction_switched = (abs(_current_num_contracts) > 0 and abs(_total_num_contracts) > 0) and ( not (numpy.sign(_current_num_contracts) == numpy.sign(_total_num_contracts) ) )
            if _direction_switched: # Direction of position changed, pnl realized
                _closed_position = _current_num_contracts
                self.todays_realized_pnl[_currency] += _closed_position * (_current_price - self.average_trade_price[_product]) * self.conversion_factor[_product] 
                self.average_trade_price[_product] = order['fill_price']
            elif abs(_total_num_contracts) > abs(_current_num_contracts): # Position increased, no pnl realized
                self.average_trade_price[_product] = (order['amount'] * order['fill_price'] + _current_num_contracts * self.average_trade_price[_product])/_total_num_contracts
            else: # Position decreased, pnl realized
                _closed_position = _current_num_contracts - _total_num_contracts
                self.todays_realized_pnl[_currency] += _closed_position * (_current_price - self.average_trade_price[_product]) * self.conversion_factor[_product]
            self.portfolio.cash -= order['cost']
        else:
            self.portfolio.cash -= (order['value'] + order['cost'])
        self.portfolio.num_shares[_product] = self.portfolio.num_shares[_product] + order['amount']

    # Computes the portfolio value at ENDOFDAY on 'date'
    def compute_mark_to_market(self, date):
        mark_to_market = self.portfolio.cash
        for product in self.products:
            if self.portfolio.num_shares[product] != 0:
                if not is_margin_product(product): # Use notional value
                    if is_future(product): # No need
                        _current_price = find_most_recent_price_future(self.bb_objects[product].dailybook, self.bb_objects[get_next_futures_contract(product)].dailybook, date)
                    else:
                        _current_price = find_most_recent_price(self.bb_objects[product].dailybook, date)
                    mark_to_market += _current_price * self.portfolio.num_shares[product] * self.conversion_factor[product] * self.currency_factor[self.product_to_currency[product]][date][1]
                else: # Use open equity
                    mark_to_market += self.portfolio.open_equity[product] * self.currency_factor[self.product_to_currency[product]][date][1]
        return mark_to_market

    def update_open_equity(self, date): # TODO change to 1 update per day: except for rollovers
        for _product in self.products:
            if self.portfolio.num_shares[_product] != 0 and is_margin_product(_product):
                if is_future(_product): # No need
                    _current_price = find_most_recent_price_future(self.bb_objects[_product].dailybook, self.bb_objects[get_next_futures_contract(_product)].dailybook, date)
                else:
                    _current_price = find_most_recent_price(self.bb_objects[_product].dailybook, date)
                self.portfolio.open_equity[_product] = (_current_price - self.average_trade_price[_product]) * self.conversion_factor[_product] * self.portfolio.num_shares[_product]
            else:
                self.portfolio.open_equity[_product] = 0

    # Called by Dispatcher
    def on_end_of_day(self, date):
        for _currency in self.currency_factor.keys():
            self.portfolio.cash += self.todays_realized_pnl[_currency] * self.currency_factor[_currency][date][1]
            self.short_term_tax_liability_realized += 0.4 * self.todays_realized_pnl[_currency] * self.currency_factor[_currency][date][1]
            # Assuming that we calculate tax in USD on realization date itself 
            self.long_term_tax_liability_realized += 0.6 * self.todays_realized_pnl[_currency] * self.currency_factor[_currency][date][1]
            self.todays_realized_pnl[_currency] = 0
        self.update_open_equity(date)
        self.short_term_tax_liability_unrealized = 0.0
        self.long_term_tax_liability_unrealized = 0.0
        for _product in self.products:
            self.short_term_tax_liability_unrealized += 0.4 * self.portfolio.open_equity[_product] * self.currency_factor[self.product_to_currency[_product]][date][1]
            self.long_term_tax_liability_unrealized += 0.6 * self.portfolio.open_equity[_product] * self.currency_factor[self.product_to_currency[_product]][date][1]
        self.compute_daily_stats(date)

    def on_tax_payment_day(self):
        s = "TAX EVENT\n"
        if self.short_term_tax_liability_realized > 0:
            self.portfolio.cash -= self.short_term_tax_liability_realized * self.short_term_tax_rate
            s += "Short term tax paid: %0.2f\n" % (self.short_term_tax_liability_realized * self.short_term_tax_rate)
            self.short_term_tax_liability_realized = 0
        if self.long_term_tax_liability_realized > 0:
            self.portfolio.cash -= self.long_term_tax_liability_realized * self.long_term_tax_rate
            s += "Long term tax paid: %0.2f\n" % (self.long_term_tax_liability_realized * self.long_term_tax_rate)
            self.long_term_tax_liability_realized = 0
        s += "Cash after tax payment: %0.2f\n" % self.portfolio.cash
        if Globals.debug_level > 0:
            Globals.positions_file.write(s)


    def on_distribution_day(self, event):
        """On a distribution day, calculate the net payout after taxes and add the money to portfolio cash
           The assumption is that for funds, capital gain is equally distributed betweeb shoirt term and long term

           Args:
               event(dict) : contains info about the distribution event
        """
        _product = event['product']
        _dt = event['dt']
        _type = event['distribution_type']
        _distribution = event['quote']
        s = "DISTRIBUTION EVENT on %s for %s of type %s: %0.2f\n" % (_dt, _product, _type, _distribution) 
        #  Tax immediately paid on dividend payout
        #  Why not do the same thing for capital gain? Commented lines which were giving tax twice
        if _type == 'DIVIDEND':
            _after_tax_net_payout = _distribution*self.portfolio.num_shares[_product]*(1 - self.dividend_tax_rate)
        elif _type == 'CAPITALGAIN': # TODO split into short term and long term based on bbg data
            _after_tax_net_payout = _distribution*self.portfolio.num_shares[_product]*(1 - (self.long_term_tax_rate + self.short_term_tax_rate)/2.0)
            #self.short_term_tax_liability_realized += _after_tax_net_payout/2.0
            #self.long_term_tax_liability_realized += _after_tax_net_payout/2.0 
        self.portfolio.cash += _after_tax_net_payout
        s += "Net payout after taxes: %0.2f\nCash after distribution = %0.2f\n" % (_after_tax_net_payout, self.portfolio.cash)
        if Globals.debug_level > 0:
            Globals.positions_file.write(s)

    # Computes the daily stats for the most recent trading day prior to 'date'
    # TOASK {gchak} Do we ever expect to run this function without current date ?
    def compute_daily_stats(self, date):
        self.date = date
        if self.total_orders > 0: # If no orders have been filled,it implies trading has not started yet
            # TODO check with gchak
            todaysValue = self.compute_mark_to_market(self.date) - (self.long_term_tax_liability_realized + self.long_term_tax_liability_unrealized)*self.long_term_tax_rate - (self.short_term_tax_liability_realized + self.short_term_tax_liability_unrealized)*self.short_term_tax_rate
            self.value = numpy.append(self.value, todaysValue)
            self.PnLvector = numpy.append(self.PnLvector, (self.value[-1] - self.value[-2]))  # daily PnL = Value of portfolio on last day - Value of portfolio on 2nd last day
            if self.value[-1] <= 0:
                _logret_today = -1000 # real value is -inf
            else:
                _logret_today = math.log(self.value[-1]/self.value[-2])
            self.daily_log_returns = numpy.append(self.daily_log_returns, _logret_today)
            self.net_log_return += self.daily_log_returns[-1]
            if self.cum_log_returns.shape[0] == 0: # If we are inserting the first element
                _cum_log_return = _logret_today
            else:
                _cum_log_return = self.cum_log_returns[-1] + _logret_today
            self.cum_log_returns = numpy.append(self.cum_log_returns, _cum_log_return)
            self.max_cum_log_return = max(self.max_cum_log_return, self.cum_log_returns[-1])
            self.current_drawdown = abs((math.exp(current_dd(self.max_cum_log_return, self.cum_log_returns)) - 1)* 100)
            self.current_loss = abs(min(0.0, (math.exp(self.net_log_return) - 1)*100.0))
            self.amount_long_transacted.append(self.todays_long_amount_transacted)
            self.amount_short_transacted.append(self.todays_short_amount_transacted)
            (notional_amounts, net_notional_exposure) = get_current_notional_amounts(self.bb_objects, self.portfolio, self.conversion_factor, self.currency_factor, self.product_to_currency, date)
            _leverage = net_notional_exposure/self.value[-1]
            self.leverage = numpy.append(self.leverage, _leverage)
            self.dates.append(self.date)
            self.print_logs(notional_amounts, self.todays_amount_transacted)
            self.todays_amount_transacted = 0.0
            self.todays_long_amount_transacted = 0.0
            self.todays_short_amount_transacted = 0.0

    def print_logs(self, notional_amounts, todays_amount_transacted):
        # Print snapshot
        if Globals.debug_level > 0:
            if self.PnLvector.shape[0] > 0:
                s = "\nPortfolio snapshot at EndOfDay %s\nPnL for today: %0.2f\nPortfolio Value: %0.2f\nCash: %0.2f\nShort Term Tax Realized: %0.2f\nLong Term Tax Realized: %0.2f\nShort Term Tax Unrealized: %0.2f\nLong Term Tax Unrealized: %0.2f\nOpen Equity: %s\nPositions: %s\nNotional Allocation: %s\nAverage Trade Price: %s\nLeverage: %0.2f\n\n" % (self.date, self.PnLvector[-1], self.value[-1], self.portfolio.cash, self.short_term_tax_liability_realized, self.long_term_tax_liability_realized, self.short_term_tax_liability_unrealized, self.long_term_tax_liability_unrealized, dict_to_string(self.portfolio.open_equity), dict_to_string(self.portfolio.num_shares), dict_to_string(notional_amounts), dict_to_string(self.average_trade_price), self.leverage[-1])
            else:
                s = "\nPortfolio snapshot at EndOfDay %s\nPnL for today: Trading has not started\nPortfolio Value: %0.2f\nCash: %0.2f\nShort Term Tax Realized: %0.2f\nLong Term Tax Realized: %0.2f\nShort Term Tax Unrealized: %0.2f\nLong Term Tax Unrealized: %0.2f\nOpen Equity: %s\nPositions: %s\nNotional Allocation: %s\nAverage Trade Price: %s\nLeverage: %0.2f\n\n" % (self.date, self.value[-1], self.portfolio.cash, self.short_term_tax_liability_realized, self.long_term_tax_liability_realized, self.short_term_tax_liability_unrealized, self.long_term_tax_liability_unrealized, dict_to_string(self.portfolio.open_equity), dict_to_string(self.portfolio.num_shares), dict_to_string(notional_amounts), dict_to_string(self.average_trade_price), self.leverage[-1])
            Globals.positions_file.write(s)
        # Print weights, leverage
        if Globals.debug_level > 1:
            s = str(self.date)
            for _product in self.products:
                _weight = notional_amounts[_product]/self.value[-1]
                s = s + ',%0.2f'% (_weight)
            Globals.weights_file.write(s + '\n')
            Globals.leverage_file.write('%s,%0.2f\n' % (self.date, self.leverage[-1]))
        # Print transacted amount
        if Globals.debug_level > 2:
            Globals.amount_transacted_file.write('%s,%0.2f\n' % (self.date, todays_amount_transacted))

    # non public function to save results to a file
    def _save_results(self):
        cPickle.dump(self.dates, Globals.returns_file)
        cPickle.dump(self.daily_log_returns, Globals.returns_file)

    def show_results(self):
        self.PnL = sum(self.PnLvector) # final sum of pnl of all trading days
        self.net_returns = (self.PnL*100.0)/self.initial_capital # final sum of pnl / initial capital
        self.annualized_PnL = 252.0 * numpy.mean(self.PnLvector)
        self.annualized_stdev_PnL = math.sqrt(252.0) * numpy.std(self.PnLvector)
        _monthly_log_returns = rollsum(self.daily_log_returns, 21)
        _quarterly_log_returns = rollsum(self.daily_log_returns, 63)
        _yearly_log_returns = rollsum(self.daily_log_returns, 252)
        self.worst_10pc_daily_returns = (math.exp(mean_lowest_k_percent(self.daily_log_returns, 10)) - 1)*100.0
        self.worst_10pc_monthly_returns = (math.exp(mean_lowest_k_percent(_monthly_log_returns, 10)) - 1)*100.0
        self.worst_10pc_quarterly_returns = (math.exp(mean_lowest_k_percent(_quarterly_log_returns, 10)) - 1) * 100.0
        self.worst_10pc_yearly_returns = (math.exp(mean_lowest_k_percent(_yearly_log_returns, 10)) - 1) * 100.0
        self.annualized_returns_percent = annualized_returns(self.daily_log_returns)
        self.annualized_stddev_returns = annualized_stdev(self.daily_log_returns)
        self.sharpe = self.annualized_returns_percent/self.annualized_stddev_returns
        self.yearly_sharpe = compute_yearly_sharpe(self.dates, self.daily_log_returns)
        self.sortino = compute_sortino(self.daily_log_returns)
        _format_strings = ','.join([' %s : %0.2f'] * len(self.yearly_sharpe))
        _yearly_sharpe_tuple = tuple(list(itertools.chain(*self.yearly_sharpe)))
        _print_yearly_sharpe = _format_strings%_yearly_sharpe_tuple
        self.skewness = ss.skew(self.daily_log_returns)
        self.kurtosis = ss.kurtosis(self.daily_log_returns)
        max_dd_log = drawdown(self.daily_log_returns)
        self.max_drawdown_percent = abs((math.exp(max_dd_log) - 1) * 100)
        self.drawdown_period, self.recovery_period = drawdown_period_and_recovery_period(self.dates, self.cum_log_returns)
        self.max_drawdown_dollar = abs(drawdown(self.PnLvector))
        self.return_by_maxdrawdown = self.annualized_returns_percent/self.max_drawdown_percent if not is_float_zero(self.max_drawdown_percent) else float('NaN')
        self.annualized_pnl_by_max_drawdown_dollar = self.annualized_PnL/self.max_drawdown_dollar
        self.ret_var10 = abs(self.annualized_returns_percent/self.worst_10pc_daily_returns)
        self.turnover_percent = turnover(self.dates, self.amount_long_transacted, self.amount_short_transacted, self.value)
        self.hit_loss_ratio = numpy.sum(numpy.where(self.daily_log_returns > 0, 1.0, 0.0))/numpy.sum(numpy.where(self.daily_log_returns < 0, 1.0, 0.0))
        self.gain_pain_ratio = numpy.sum(self.daily_log_returns)/numpy.sum(numpy.where(self.daily_log_returns < 0, -self.daily_log_returns, 0.0))
        self.max_num_days_no_new_high = compute_max_num_days_no_new_high(self.dates, self.PnLvector)
        self.losing_month_streak = compute_losing_month_streak(self.dates, self.daily_log_returns)
        _extreme_days = get_extreme_days(self.dates, self.daily_log_returns, 5)
        _extreme_weeks = get_extreme_weeks(self.dates, self.daily_log_returns, 5)
        if len(self.leverage) > 0:
            _leverage_params = (min(self.leverage), max(self.leverage), numpy.mean(self.leverage), numpy.std(self.leverage))
        else:
            _leverage_params = (0, 0, 0, 0)
        self._save_results()

        _stats = _extreme_days + _extreme_weeks 
        _stats += ("\nInitial Capital = %.2f\nNet PNL = %.2f \nTrading Cost = %.2f\nNet Returns = %.2f%%\nAnnualized PNL = %.2f\nAnnualized_Std_PnL = %.2f\nAnnualized_Returns = %.2f%% \nAnnualized_Std_Returns = %.2f%% \nSharpe Ratio = %.2f \nSortino Ratio = %.2f\nSkewness = %.2f\nKurtosis = %.2f\nDML = %.2f%%\nMML = %.2f%%\nQML = %.2f%%\nYML = %.2f%%\nMax Drawdown = %.2f%% \nDrawdown Period = %s to %s\nDrawdown Recovery Period = %s to %s\nMax Drawdown Dollar = %.2f \nAnnualized PNL by drawdown = %.2f \nReturn_drawdown_Ratio = %.2f\nReturn Var10 ratio = %.2f\nYearly_sharpe = " + _print_yearly_sharpe + "\nHit Loss Ratio = %0.2f\nGain Pain Ratio = %0.2f\nMax num days with no new high = %d from %s to %s\nLosing month streak = Lost %0.2f%% in %d months from %s to %s\nTurnover = %0.2f%%\nLeverage = Min : %0.2f, Max : %0.2f, Average : %0.2f, Stddev : %0.2f\nTrading Cost = %0.2f\nTotal Money Transacted = %0.2f\nTotal Orders Placed = %d\n") % (self.initial_capital, self.PnL, self.trading_cost, self.net_returns, self.annualized_PnL, self.annualized_stdev_PnL, self.annualized_returns_percent, self.annualized_stddev_returns, self.sharpe, self.sortino, self.skewness, self.kurtosis, self.worst_10pc_daily_returns, self.worst_10pc_monthly_returns, self.worst_10pc_quarterly_returns, self.worst_10pc_yearly_returns, self.max_drawdown_percent, self.drawdown_period[0], self.drawdown_period[1], self.recovery_period[0], self.recovery_period[1], self.max_drawdown_dollar, self.annualized_pnl_by_max_drawdown_dollar, self.return_by_maxdrawdown, self.ret_var10, self.hit_loss_ratio, self.gain_pain_ratio, self.max_num_days_no_new_high[0], self.max_num_days_no_new_high[1], self.max_num_days_no_new_high[2], self.losing_month_streak[1], self.losing_month_streak[0], self.losing_month_streak[2], self.losing_month_streak[3], self.turnover_percent, _leverage_params[0], _leverage_params[1], _leverage_params[2], _leverage_params[3], self.trading_cost, self.total_amount_transacted, self.total_orders)
        _stats += '\nBenchmarks:\n'
        for benchmark in self.benchmarks:
            _stats += get_benchmark_stats(self.dates, self.daily_log_returns, benchmark) # Returns a string of benchmark stats
        print _stats
        Globals.stats_file.write(_stats)
        #return JsonParser().sim_to_json(Globals.config_file, self.dates, self.daily_log_returns, self.leverage, parse_results(_stats)) # Return the json object corresponding to the simulation
        JsonParser().dump_sim_to_db(Globals.config_file, self.dates, self.daily_log_returns, self.leverage, parse_results(_stats))
        return None
