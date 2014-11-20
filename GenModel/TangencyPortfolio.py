#!/usr/bin/env python
import sys
import os
import datetime
import numpy as np
from numpy.linalg import inv
import pandas as pd

def main():
    if len ( sys.argv ) <= 1  :
        print "python TangencyPortfolio.py returns-file"
    dateparse = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date()
    df = pd.read_csv(sys.argv[1],header=0,parse_dates =['Date'],date_parser=dateparse)
    df = df.set_index('Date')
    cov_mat = np.asmatrix(df.cov()) # covariance matrix
    mu = np.asmatrix(df.mean()).T # expected returns
    unit = np.asmatrix( np.ones( mu.shape[0] ) ) # unit vector
    weights = (inv(cov_mat)*mu/float(unit*inv(cov_mat)*mu)) # weights = ((cov-1)*mu)/scaling_factor , where scaling factor = unit*(cov-1)*mu
    weights = np.squeeze(np.asarray(weights))

    # Display in config format
    s=''
    for i in range(0,len(df.columns)):
        s = s + df.columns[i].split('.')[1]+','+str(weights[i])+' '
    print s

if __name__ == '__main__':
    main();
