import sys
import math
import numpy
import pandas
from datetime import datetime

def inverse_volatility(returns):
    return 1.0/numpy.std(returns)

def sharpe(returns):
    return (math.exp(252.0 * numpy.mean(returns)) - 1)/(math.exp(math.sqrt(252.0) * numpy.std(returns)) - 1)

def __main__():
    dateparse = lambda x: datetime.strptime(x, '%Y-%m-%d').date()
    log_returns = pandas.read_csv(sys.argv[1], parse_dates=['date'], date_parser = dateparse, header=0)
    log_returns.set_index(['date'], inplace=True)
    #wts = numpy.array(log_returns.apply(inverse_volatility, axis=0))
    wts = numpy.array(log_returns.apply(sharpe, axis=0))
    wts = wts/sum(wts) # Normalize to sum to 1, all values are +ve
    print log_returns.columns.values, wts

if __name__ == '__main__':
    if len ( sys.argv ) < 2 :
        sys.exit("returns_file")
    __main__()
