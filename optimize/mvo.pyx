#!/usr/bin/env python
import sys
import os
import subprocess
import ConfigParser
import pandas as pd
import numpy as np
from numpy import hstack,vstack
from shutil import copyfile
from math import sqrt,exp
from cvxopt import matrix
from cvxopt.blas import dot
from cvxopt.solvers import qp, options
from datetime import datetime
from utils import get_logreturns,change_weights,parse_results

# Helper functions to succinctly represent constraints for optimizer 
# Helper function lag(x,k)
# Introduces a lag of k in a series x with leading zeros
# lag([1,2,3,4],2) = [0,0,1,2]
def lag(x, k):
    if k == 0:
        return x
    elif k > len(x):
        return len(x)*[0]
    else:
        return k*[0]+x[0:-k]

# Helper function shift_vec(x,m)
# Produces a matrix of vectors each with a lag i where i ranges from 0 to m-1
# shift_vec([1,2,3],3) = [[1,2,3],[0,1,2],[0,0,1]]'
def shift_vec(x,m):
    n = len(x)
    mat = [lag(x,i) for i in range(0,m)]
    mat = matrix(mat, (n, m))
    return mat

# Function that calculates the efficient frontier  
# by minimizing (Variance - tolerance * expected returns)
# with the given constraints
# sum(abs(weights)) <= 5
# Input: returns - matrix containing log daily returns for n securities.
#        tolerance_limit - upper bound of tolerance in function to be minimized
#        max_allocation - maximum weight to be allocated to one security
#                         (set low threshold to diversify)
# Output: frontier - matrix of optimal weight for each value of tolerance with 
#                    performance stats exp.returns, std.dev, sharpe ratio
def efficient_frontier(returns, tolerance_delta=0.0005, max_allocation=0.5):
    n = returns.shape[0] # Number of products
    k = returns.shape[1] # Number of days
    covariance = np.cov(returns)
    expected_returns = np.mean(returns, axis=1)

    # Setup inputs for optimizer
    # min(-d^T b + 1/2 b^T D b) with the constraints A^T b <= b_0
    # Constraint: sum(abs(weights)) <= 5 is non-linear
    # To make it linear introduce a dummy weight vector y = [y1..yn]
    # w1<y1,-w1<y1,w2<y2,-w2<y2,...
    # y1,y2,..,yn > 0
    # y1 + y2 + ... + yn <= 5
    # Optimization will be done to find both w and y i.e 29+29 weights
    # Dmat entries for y kept low to not affect minimzing function as much as possible
    # Not kept 0 to still keep Dmat as semi-definite
    dummy_var_dmat = 0.000001*np.eye(n)
    Dmat = vstack(( hstack(( covariance, matrix(0.,(n,n)) )), hstack(( matrix(0.,(n,n)), dummy_var_dmat)) ))
    # Constraint 1:  y1 + y2 + ... + yn <= 5
    Amat = vstack(( matrix (0, (n,1)), matrix (1, (n,1)) ))
    bvec = [5]
    # Constraint 2-30:  y1, y2 ,..., yn >= 0
    Amat = hstack(( Amat, vstack(( matrix(0,(n,n)),-1*np.eye(n) )) ))
    bvec = bvec + n*[0]
    # Constraint 31-59:  y1, y2 ,..., yn <= max_allocation
    Amat = hstack(( Amat, vstack(( matrix(0,(n,n)), np.eye(n) )) ))
    bvec = bvec + n*[max_allocation]
    # Constraint 60-88:  -w1 <= y1, -w2 <= y2, ..., -wn <= yn
    dummy_wt_constraint1 = [-1] + (n-1)*[0] + [-1] + (n-1)*[0]
    Amat = hstack(( Amat,shift_vec(dummy_wt_constraint1,n) ))
    bvec = bvec + n*[0]
    #Constraint 89-117:  w1 <= y1, w2 <= y2, ..., wn <= yn
    dummy_wt_constraint2 = [1] + (n-1)*[0] + [-1] + (n-1)*[0]
    Amat = hstack(( Amat,shift_vec(dummy_wt_constraint2,n) ))
    bvec = bvec + n*[0]

    # Convert all NumPy arrays to CVXOPT matrics
    Dmat = matrix(Dmat)
    bvec = matrix(bvec,(len(bvec),1))
    Amat = matrix(Amat.T)
    dvec = matrix( hstack(( expected_returns,n*[0] )).T )
	
    # Iterate over different values of tolerance
    N = 10
    mus = [ -0.0001-t*10*tolerance_delta for t in range(N) ]
    portfolios = [ qp(Dmat, mu*dvec, Amat, bvec)['x'] for mu in mus ]
	
    return portfolios

def cross_validate( _config_file, _trade_products, _start_date, _end_date, folds ):
    # Perform rolling cross-validation
    # Training set divided into 10 sets
    # Iteration 1: Train 1, Test on 2
    # Iteretion 2: Train 1+2, Test on 3 
    # Iteration 3: Train 1+2+3, Test on 4
    # ...
    # Iteration N: Train 1+2+..+N-1, Test on N
		
    weights_cv = []
    perf_stats_cv= 10 * [0]
    _returns_data_filename = 'Data/returns.csv'

    start_date = datetime.combine( datetime.strptime( _start_date, "%Y-%m-%d" ).date(), datetime.max.time() ).date()
    end_date = datetime.combine( datetime.strptime( _end_date, "%Y-%m-%d" ).date(), datetime.max.time() ).date()

    d = ( end_date - start_date )/folds
    start_dates = [ i * d + start_date for i in range(folds+1) ]
    for i in range(1,folds):
        print 'FOLD %d'%i
        #Create test and training dates for n fold rolling cross validation
        train_dates = [ str(start_dates[0]), str(start_dates[i]) ]
        test_dates = [ str(start_dates[i]), str(start_dates[i+1]) ]
        
        ( _logret_matrix, _products_order ) = get_logreturns( _config_file, _trade_products, train_dates[0], train_dates[1], _returns_data_filename )
        _logret_matrix = _logret_matrix.T
        k = _logret_matrix.shape[0]
        portfolios = efficient_frontier(_logret_matrix)
        portfolios = [np.array(x[0:k]) for x in portfolios]

        performance_stats=[]
        i=0
        # Run the strategy
        for x in portfolios:
            print 'Simulating %dth Portfolio'%i
            i=i+1
            change_weights( _config_file, _products_order, x[:,0] )
            proc = subprocess.Popen(['python', 'simulator.py', _config_file, test_dates[0], test_dates[1] ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            performance_stats.append( parse_results( proc.communicate()[0] ) )
            
        # Performance stats chosen as SharpeRatio + 0.20*return_dd_ratio
        perf_stats = [x['Sharpe Ratio'] + 0.20*x['Return_drawdown_Ratio'] for x in performance_stats]		
        # Add to previous array of portfolios and performance strategy
        perf_stats_cv = [x + y for x, y in zip(perf_stats_cv, perf_stats)]
			
    # Choose that value of mu which performs best in out of sample data on an average
    chosen_portfolio = np.argmax(perf_stats_cv)

    # Train it on the full training se using the chosen value of mu to get the final weights
    ( _logret_matrix, _products_order ) = get_logreturns( _config_file, _trade_products, _start_date, _end_date, _returns_data_filename )
    _logret_matrix = _logret_matrix.T
    k = _logret_matrix.shape[0]
    final_weights = np.array(efficient_frontier(_logret_matrix)[chosen_portfolio][0:k])
    return final_weights[:,0]	

def main():
    if ( len ( sys.argv ) <= 2 ) :
        print "Arguments needed: config_file weights_file <start_date end_date>"
        sys.exit(0)

    # Get handle of config file
    _config_file = sys.argv[1]
    _config = ConfigParser.ConfigParser()
    _config.readfp( open( _config_file, 'r' ) )

    # Read product list from config file
    _trade_products = _config.get( 'Products', 'trade_products' ).strip().split(",")

    _weights_filename = sys.argv[2]
    if len ( sys.argv ) == 5 :
        _start_date = sys.argv[3]
        _end_date = sys.argv[4]
    else :
        _start_date = _config.get( 'Dates', 'start_date' )
        _end_date = _config.get( 'Dates', 'end_date' )

    # Get final weights after optimization and 5 fold rolling cross-validation
    weights = cross_validate( _config_file, _trade_products, _start_date, _end_date, 5 )

    # Output weights to file
    s=''
    for symbol,weight in zip(_trade_products,weights):
        s = s + symbol + ',' + str(weight) + ' '  
    with open(_weights_filename,'w') as f:
        f.write( s )    

if __name__ == '__main__':
    main();
