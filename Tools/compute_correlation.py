import pickle
import sys
from numpy import *
import scipy.stats as ss
import datetime

def filter_series(dates_returns_1,dates_returns_2):
    dates1 = [item[0] for item in dates_returns_1]
    dates2 = [item[0] for item in dates_returns_2]
    returns1 = array([item[1] for item in dates_returns_1]).astype(float)
    returns2 = array([item[1] for item in dates_returns_2]).astype(float)
    all_dates = [dates1,dates2]
    all_series = [returns1,returns2]
    intersected_dates = list(set(all_dates[0]).intersection(*all_dates))
    intersected_dates.sort()
    filtered_series = []
    for i in xrange(0,len(all_series)):
        Indexes = sort(searchsorted(all_dates[i],intersected_dates))
        filtered_series.append(all_series[i][Indexes])
    filtered_series = (array(filtered_series).T).astype(float)
    return (filtered_series[:,0],filtered_series[:,1])

def compute_correlation(dates_returns_1,dates_returns_2,name_1,name_2):
    print '\n\nCalculating correlation between %s and %s'%(name_1,name_2)
    print 'Length of Vectors : %d and %d'%(len(dates_returns_1),len(dates_returns_2))
    (r1,r2) = filter_series(dates_returns_1,dates_returns_2)
    print 'Length of Vectors used for calculating correlation: %d'%len(r1)
    corr = ss.stats.pearsonr(r1,r2)
    print 'Correlation Value: %.10f'%corr[0]

def __main__():
    if len(sys.argv) <= 2 :
        sys.exit('To Run: python compute_correlation.py return_series_1.txt return_series_2.txt')
    _file1,_file2 = sys.argv[1], sys.argv[2]
    f1,f2 = open(_file1, 'rb'), open(_file2, 'rb')
    _dates_returns_1, _dates_returns_2 = pickle.load(f1), pickle.load(f2)
    compute_correlation(_dates_returns_1,_dates_returns_2,_file1.split('/')[-2],_file2.split('/')[-2])

__main__()
