# cython: profile=True
#!/usr/bin/env python
import sys
import os
import subprocess
import ConfigParser
import pandas as pd
from numpy import *
from scipy.optimize import minimize
from shutil import copyfile
from utils import get_logreturns,change_weights

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
    _target_risk = _config.getfloat( 'Strategy', 'target_risk' )

    _weights_filename = sys.argv[2]

    if len ( sys.argv ) == 5 :
        _start_date = sys.argv[3]
        _end_date = sys.argv[4]
    else :
        _start_date = _config.get( 'Dates', 'start_date' )
        _end_date = _config.get( 'Dates', 'end_date' )

    #Run gendata to get the logreturn matrix
    ( _logret_matrix, products_order ) = get_logreturns( _config_file, _trade_products, _start_date, _end_date, 'Data/returns.csv' )

    _cov_mat = cov( _logret_matrix.T )
    _risk = std(_logret_matrix,axis=0,ddof=1) # added ddof to match the assumptions of cov
    _annualized_risk = 100.0*(exp(sqrt(252.0)*_risk)-1)
    _w = 1.0/(_annualized_risk)
    _w = _w/sum(abs(_w))

    prc = 100.0*(_w * array(asmatrix( _cov_mat )*asmatrix( _w ).T)[:,0])/((asmatrix( _w )*asmatrix( _cov_mat )*asmatrix( _w ).T))
    print 'INITIAL PERCENTAGE RISK CONTRIBUTIONS:',prc

    print 'INITIAL RP WEIGHTS',_w
    # Minimize: sum_i[ abs( trc_i -mean(trc) ) ]
    def rosen(_w):
        _cov_vec = array(asmatrix( _cov_mat )*asmatrix( _w ).T)[:,0]
        _trc = _w*_cov_vec
        return sum( abs( _trc - mean(_trc)) )

    cons =  {'type':'eq', 'fun': lambda x: sum(abs(x)) - 1} 
    _w = minimize ( rosen, _w, method='SLSQP',constraints=cons,options={'ftol': 0.0000000000000000000000000001,'disp': True,'maxiter':10000 } ).x
    
    _annualized_stddev_of_portfolio = 100.0*(exp(sqrt(252.0*(asmatrix( _w )*asmatrix( _cov_mat )*asmatrix( _w ).T))[0,0])-1)
    _w_new = _w*(_target_risk/_annualized_stddev_of_portfolio) 
    
    print 'SUM OF ABS(WEIGHTS): %0.2f'%sum( abs( _w_new ) )
    print 'WEIGHTS:',_w_new
    _cov_vec = array(asmatrix( _cov_mat )*asmatrix( _w_new ).T)[:,0]
    _trc = _w_new*_cov_vec

    prc = 100.0*(_w_new * array(asmatrix( _cov_mat )*asmatrix( _w_new ).T)[:,0])/((asmatrix( _w_new )*asmatrix( _cov_mat )*asmatrix( _w_new ).T))
    print 'FINAL PERCENTAGE RISK CONTRIBUTIONS: ',prc

    # Output weights to file and change the weights in config
    change_weights( _config_file, products_order, _w_new )
    s='' 
    for symbol,weight in zip(products_order,_w_new):
        s = s + symbol + ',' + str(weight) + ' '  
    with open(_weights_filename,'w') as f:
        f.write( s )    

if __name__ == '__main__':
    main();
