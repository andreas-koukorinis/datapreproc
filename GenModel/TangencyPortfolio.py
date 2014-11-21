#!/usr/bin/env python
import sys
import os
import datetime
import numpy as np
from numpy.linalg import inv
import pandas as pd

def main():
    if len ( sys.argv ) <= 1  :
        sys.exit("python TangencyPortfolio.py returns-file")
    dateparse = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date()
    df = pd.read_csv(sys.argv[1],header=0,parse_dates =['Date'],date_parser=dateparse)
    df = df.set_index('Date')
    cov_mat = np.asmatrix(df.cov()) # covariance matrix
    mu = np.asmatrix(df.mean()).T # expected returns
    unit = np.asmatrix( np.ones( mu.shape[0] ) ) # unit vector
    w = (inv(cov_mat)*mu/float(unit*inv(cov_mat)*mu)) # weights = ((cov-1)*mu)/scaling_factor , where scaling factor = unit*(cov-1)*mu
    max_sharpe = (w.T*mu)/np.sqrt(w.T*cov_mat*w)
    max_sharpe1 = (w.T*((np.exp(mu)-1)*100.0))/((np.exp(np.sqrt(w.T*cov_mat*w))-1)*100.0)
    print 'Max Sharpe : %f,%f'%(max_sharpe,max_sharpe1)
    #print np.sqrt(mu.T*inv(cov_mat)*mu)

    w = np.squeeze(np.asarray(w))

    # Display in config format
    s=''
    for i in range(0,len(df.columns)):
        s = s + df.columns[i].split('.')[1]+','+str(w[i])+' '
    print s

if __name__ == '__main__':
    main();
