import pickle
import sys
from numpy import *
import scipy.stats as ss
import datetime
from utils.regular import filter_series

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
