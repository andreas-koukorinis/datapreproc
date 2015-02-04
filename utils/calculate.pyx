# cython: profile=True
import sys
import numpy
import datetime
import itertools
import scipy.stats as ss
from regular import is_future, get_next_futures_contract, is_margin_product, filter_series

def get_current_prices( bb_objects ):
    current_prices = {}
    for product in bb_objects.keys():
        if len(bb_objects[product].dailybook) >= 1:
            current_prices[product] = bb_objects[product].dailybook[-1][1]
    return current_prices

#Getthe current worth of the portfolio based on the most recent daily closing prices
def get_mark_to_market(date, current_price, conversion_factor, currency_factor, product_to_currency, performance_tracker, current_portfolio):
    mark_to_market = current_portfolio['cash']
    num_shares = current_portfolio['num_shares']
    open_equity = current_portfolio['open_equity']
    performance_tracker.update_open_equity(date)
    for product in current_price.keys():
        if not is_margin_product(product):
            mark_to_market += (current_price[product] * conversion_factor[product] * currency_factor[product_to_currency[product]][date][1] * num_shares[product])
        else:
            mark_to_market += open_equity[product] * currency_factor[product_to_currency[product]][date][1]
    return mark_to_market

def get_current_notional_amounts(bb_objects, portfolio, conversion_factor, currency_factor, product_to_currency, date):
    notional_amount = {}
    net_notional_exposure = 0.0
    for product in portfolio.num_shares.keys():
        if portfolio.num_shares[product] != 0:
            if is_future(product):
                _price = find_most_recent_price_future(bb_objects[product].dailybook, bb_objects[get_next_futures_contract(product)].dailybook, date)
            else:
                _price = find_most_recent_price(bb_objects[product].dailybook, date)
            notional_amount[product] = _price * portfolio.num_shares[product] * conversion_factor[product] * currency_factor[product_to_currency[product]][date][1]
        else:
            notional_amount[product] = 0.0
        net_notional_exposure += abs(notional_amount[product])
    return (notional_amount, net_notional_exposure)

def convert_daily_returns_to_yyyymm_monthly_returns_pair(dates, returns):
    yyyymm = [ date.strftime("%Y") + '-' + date.strftime("%m") for date in dates]
    yyyymm_returns = zip(yyyymm, returns)
    monthly_returns = []
    for key, rows in itertools.groupby(yyyymm_returns, lambda x : x[0]):
        monthly_returns.append( (key, sum(x[1] for x in rows) ) )
    return monthly_returns

def compute_correlation(labels_and_returns_1, labels_and_returns_2):
    if len(labels_and_returns_1) <= 1 or len(labels_and_returns_2) <= 1:
        return 0
    filtered_labels_and_returns_1, filtered_labels_and_returns_2 = filter_series(labels_and_returns_1, labels_and_returns_2)
    if len(filtered_labels_and_returns_1) != len(labels_and_returns_1) or len(filtered_labels_and_returns_2) != len(labels_and_returns_2): # If some records were filtered out
        pass#print '%d vs %d vs %d vs %d'%(len(filtered_labels_and_returns_1), len(labels_and_returns_1), len(filtered_labels_and_returns_2), len(labels_and_returns_2))
    if len(filtered_labels_and_returns_1) <= 1 or len(filtered_labels_and_returns_2) <= 1:
        return 0
    corr = ss.stats.pearsonr(filtered_labels_and_returns_1, filtered_labels_and_returns_2)
    return corr[0]

def compute_daily_log_returns(prices):
    return numpy.log(prices[1:]/prices[:-1])

#Find the latest price prior to 'date'
def find_most_recent_price(book, date):
    if len(book) < 1:
        sys.exit('ERROR: warmupdays not sufficient')
    elif book[-1][0].date() <= date:
        return book[-1][1]
    else:
        return find_most_recent_price(book[:-1], date)

#Find the latest price prior to 'date' for futures product
def find_most_recent_price_future(book1, book2, date):
    if len(book1) < 1:
        sys.exit('ERROR: warmupdays not sufficient')
    elif book1[-1][0].date() <= date and book1[-1][2]: #If the day was settlement day,then use second futures contract price
        return book2[-1][1]
    elif book1[-1][0].date() <= date and not book1[-1][2]: #If the day was not settlement day,then use first futures contract price
        return book1[-1][1]
    else:
        return find_most_recent_price_future(book1[:-1], book2[:-1], date)
