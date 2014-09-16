import csv
from numpy import *

# read log returns data directly from csv file
def load_data(file_,typ):
    data = []
    with open(file_, 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data.append(row)
    data = array(data)
    data = data.astype(typ)
    return data

# Compute Returns based on prices and Specific Symbol (data is filtered beforehand based on common dates)
# Prices : should be n*k 2d array where n is the number of trading days and k is the number of instruments
def compute_returns(prices):
    prices=prices.astype(float)
    returns = zeros([prices.shape[0]-1,prices.shape[1]]) 
    for i in xrange(1,prices.shape[0]):
        returns[i-1,:] = log(prices[i,:]/prices[i-1,:])
    return returns

#To be generallized
# Computes Returns based on prices and Specific Symbol (data is assumed to be filtered beforehand based on common dates)
# Prices : should be n*k 2d array where n is the number of trading days and k is the number of instruments
# Specific : Specific symbol corresponding to the instrument 
def compute_returns_specs(prices,specific):
    prices=prices.astype(float)
    returns = zeros([prices.shape[0]-1,prices.shape[1]]) 
    for i in xrange(1,prices.shape[0]):
        returns[i-1,0] = where(specific[i,0]==specific[i-1,0],log(prices[i,0]/prices[i-1,0]),log(prices[i,0]/prices[i-1,1]))
        returns[i-1,2] = where(specific[i,1]==specific[i-1,1],log(prices[i,2]/prices[i-1,2]),log(prices[i,2]/prices[i-1,3]))
    returns = array([returns[:,0],returns[:,2]])
    returns = returns.T
    returns= returns.astype(float)
    return returns



