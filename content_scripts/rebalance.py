import pandas as pd
import math
import numpy as np
import itertools

# Helper functions from performance_utils of stratdev to get stats
def drawdown(returns):
    """Calculates the global maximum drawdown i.e. the maximum drawdown till now"""
    if returns.shape[0] < 2:
        return 0.0
    cum_returns = returns.cumsum()
    return -1.0*max(np.maximum.accumulate(cum_returns) - cum_returns) # Will return a negative value

def annual_std(returns):
    return np.std([sum(returns[lo:lo + 252]) for lo in range(len(returns) - 252 +1)])

def average_yearly_returns(dates,returns):
    yyyy = [ date.strftime("%Y") for date in dates]
    yyyy_returns = zip(yyyy, returns)
    yearly_returns = []
    for key, rows in itertools.groupby(yyyy_returns, lambda x : x[0]):
        _returns = np.array([x[1] for x in rows])
        yearly_returns.append((math.exp(252.0*np.mean(_returns)) - 1) * 100)
    return np.mean(yearly_returns)

# Input CSV with date, prod1, prod2, prod3 ... prodn
# with prices for each date for each product
# Sample dataset present in rebalance_dataset.csv 
data = pd.DataFrame.from_csv('rebalance_dataset.csv', infer_datetime_format=True)

# Pairs of products one uses to rebalances the portfolio
# CSV file for each pair will be output
prods = [('VTSMX','VBLTX'),('VTSMX','IEMG'),('VTI','IEMG'),('VTI','VBLTX'),('MUB','VTI'),('MUB','IEMG'),('MUB','VTSMX'), \
         ('VTSMX','VEA'),('VBLTX','VEA'),('VTI','VEA'),('IEMG','VEA'),('MUB','VEA'),('VBR','VEA'), \
         ('VTSMX','VTV'),('VBLTX','VTV'),('VTI','VTV'),('IEMG','VTV'),('MUB','VTV'),('VBR','VTV'),('VEA','VTV'), \
         ('VBR','IEMG'),('VBR','MUB'),('VBR','VTI'),('VBR','VTSMX'),('VBR','VBLTX'), \ 
         ('SPTR','VEA'),('SPTR','VTV'),('SPTR','VBR'),('SPTR','VTI'),('SPTR','IEMG'),('SPTR','MUB'),('VBR','VIX')]

for (prod1,prod2) in prods:
    portfolios = []
    capital = 100
    ret = []
    sdev = []
    maxdd = []
    pt = []
    sh = []
    rdd = []
    an_sdev = []
    an_sh = []
    yr_rets = []

    for x in xrange(101):
            x = 0.01 * x
            capital = 100
            log_returns = []

            for i in xrange(len(data.index)-1):
                
                m_prod1 = x * capital
                m_prod2 = capital - m_prod1
                
                n_prod1 = m_prod1 / float(data[prod1][i])
                n_prod2 = m_prod2 / float(data[prod2][i])
                
                pnl = n_prod1 * (float(data[prod1][i+1]) - float(data[prod1][i])) + n_prod2 * (float(data[prod2][i+1]) - float(data[prod2][i]))
                log_returns.append(math.log(1+pnl/capital))
                capital = capital + pnl

            ret.append((math.exp(252.0*np.mean(log_returns)) - 1) * 100)
            sdev.append((math.exp(math.sqrt(252.0)*np.std(log_returns)) - 1) * 100)
            maxdd.append(abs((math.exp(drawdown(np.array(log_returns))) - 1) * 100))
            pt.append(x)
            sh.append(ret[-1]/sdev[-1])
            rdd.append(ret[-1]/maxdd[-1])
            an_sdev.append((math.exp(annual_std(log_returns))-1)* 100)
            an_sh.append(ret[-1]/an_sdev[-1])
            yr_rets.append(average_yearly_returns(data.index,log_returns))
            portfolios.append(capital)

    # Create results dataframe
    df = pd.DataFrame()
    df['% of product 1 in portfolio'] = pt
    df['Portfolio Value'] = portfolios
    df['Annualized Returns'] = ret
    df['Annualized Stdev'] = sdev
    df['Max drawdown'] = maxdd
    df['Sharpe Ratio'] = sh
    df['Return to drawdown ratio'] = rdd
    df['Annual std'] = an_sdev
    df['Annual sharpe'] = an_sh
    df['Average Yearly Returns'] = yr_rets
    df.to_csv('portfolios_'+prod1+'_'+prod2+'.csv',index=False,float_format="%0.4f")
    
    # Output this into a csv file which can be imported into Google Sheets or Excel
    print prod1, prod2, " stats,,,,,"
    index = portfolios.index(max(portfolios))
    print "Max Portfolio Value,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f" % (max(portfolios), 0.01 * index, yr_rets[index], maxdd[index], an_sdev[index], sh[index])
    index = ret.index(max(ret))
    print "Max Annualized Returns,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f" % (max(ret), 0.01 * index, yr_rets[index], maxdd[index], an_sdev[index], sh[index])
    index = sdev.index(min(sdev))
    print "Min Annualized Stdev,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f" % (min(sdev), 0.01 * index, yr_rets[index], maxdd[index], an_sdev[index], sh[index])
    index = maxdd.index(min(maxdd))
    print "Min Max DD,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f" % (min(maxdd), 0.01 * index, yr_rets[index], maxdd[index], an_sdev[index], sh[index])
    index =  sh.index(max(sh))
    print "Max Sharpe Ratio,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f" % (max(sh), 0.01 * index, yr_rets[index], maxdd[index], an_sdev[index], sh[index])
    index = rdd.index(max(rdd))
    print "Max Return to drawdown ratio,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f" % (max(rdd), 0.01 * index, yr_rets[index], maxdd[index], an_sdev[index], sh[index])
    index = an_sh.index(max(an_sh))
    print "Max annual sharpe,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f,%0.2f" % (max(an_sh), 0.01 * index, yr_rets[index], maxdd[index], an_sdev[index], sh[index])

