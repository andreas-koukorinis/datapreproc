import sys
import numpy
import datetime
import itertools
import scipy.stats as ss

def filter_series(dates_returns_1,dates_returns_2):
    dates1 = [item[0] for item in dates_returns_1]
    dates2 = [item[0] for item in dates_returns_2]
    returns1 = numpy.array([item[1] for item in dates_returns_1]).astype(float)
    returns2 = numpy.array([item[1] for item in dates_returns_2]).astype(float)
    all_dates = [dates1,dates2]
    all_series = [returns1,returns2]
    intersected_dates = list(set(all_dates[0]).intersection(*all_dates))
    intersected_dates.sort()
    filtered_series = []
    for i in xrange(0,len(all_series)):
        Indexes = numpy.sort(numpy.searchsorted(all_dates[i],intersected_dates))
        filtered_series.append(all_series[i][Indexes])
    filtered_series = (numpy.array(filtered_series).T).astype(float)
    return (filtered_series[:,0],filtered_series[:,1])

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