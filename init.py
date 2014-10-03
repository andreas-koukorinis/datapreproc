import csv
import MySQLdb
from numpy import *
from getData import getPrice,getSpec

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

# Computes Log returns for symbol 'product' from 'startdate' to 'enddate'
# Fetches the (dates,prices) of the products with symbols mentioned in the 'products' list
# ASSUMPTION : If the symbol contains numeric at the end,then uses specific symbol to calculate the correct returns from prices otherwise simply calculates returns
# Passes returns and dates to filter_returns function to remove non-common entries among different products
def getLogReturns(products,startdate,enddate):
    all_dates=[]
    all_returns=[]
    for prod_ in products:
        sym = prod_.rstrip('1234567890')
        if(len(sym)==len(prod_)):
            (d,p) = getPrice(sym,startdate,enddate)							#If the product has same specific and generic symbol
            r = compute_returns(p)
            d = d[1:]
        else:							
            (d1,p1) = getPrice(sym+"1",startdate,enddate)
            (d2,p2) = getPrice(sym+"2",startdate,enddate)
            specs   = getSpec(sym,startdate,enddate)
            (d,r)   = (d1[1:],compute_returns_specs(array([p1,p2]).T,specs))
        all_dates.append(d)
        all_returns.append(r)
    return filter_returns(all_dates,all_returns)

#Returns an n*k array containing the returns for all 'k' instruments which are available on the common trading days 
# all_dates : list of lists where each list contains the dates for which data is available for a particular instrument
# all_returns : list of lists where each list contains the returns for the corresponding list in 'all_dates' for a particular instrument
def filter_returns(all_dates,all_returns):
    if(len(all_returns)==1):
        return (array(all_returns).T).astype(float)
    intersected_dates = list(set(all_dates[0]).intersection(*all_dates))
    filteredret = []
    for i in xrange(0,len(all_returns)):
        Indexes = sort(searchsorted(all_dates[i],intersected_dates))
        filteredret.append(all_returns[i][Indexes])
    filteredret = (array(filteredret).T).astype(float)
    return filteredret

#Returns 2 arrays of (Dates,ModifiedPrices) corresponding to symbol 'product' between dates 'startdate' and 'enddate'
def getModifiedPrice(product,startdate,enddate):
    ret = getLogReturns([product],startdate,enddate)    
    (dates,prices) = getPrice(product,startdate,enddate) 
    modified_prices = zeros(prices.shape[0])
    modified_prices[modified_prices.shape[0]-1] = prices[modified_prices.shape[0]-1]
    modified_prices[0:modified_prices.shape[0]-1] = prices[1:modified_prices.shape[0]]/exp(ret[:,0])
    print ret[:,0]
    return (dates,modified_prices)
        
# Compute Returns based on prices and Specific Symbol (data is filtered beforehand based on common dates)
# Prices : should be n*k 2d array where n is the number of trading days and k is the number of instruments
def compute_returns(prices):
    prices=prices.astype(float)
    returns = zeros([prices.shape[0]-1,prices.shape[1]]) 
    for i in xrange(1,prices.shape[0]):
        returns[i-1,:] = log(prices[i,:]/prices[i-1,:])
    return returns

# Computes Returns based on prices and Specific Symbol (data is assumed to be filtered beforehand based on common dates)
# Prices : should be n*k 2d array where n is the number of trading days and k is the number of instruments
# Specific : Specific symbol corresponding to the instrument 
def compute_returns_specs(prices,specific):
    prices=prices.astype(float)
    returns = zeros(prices.shape[0]-1) 
    for i in xrange(1,prices.shape[0]):
        returns[i-1] = where(specific[i]=="" or specific[i-1] =="" or specific[i]==specific[i-1],log(prices[i,0]/prices[i-1,0]),log(prices[i,0]/prices[i-1,1]))
    returns= returns.astype(float)
    return returns

#getLogReturns(['TY1','ES1'],'20140901','20140905')
#(d,m) = getModifiedPrice('TY1','20140801','20140830')
