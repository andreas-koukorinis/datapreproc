#!/usr/bin/env python

import sys
import ConfigParser
import numpy as np
from scipy.optimize import minimize
import subprocess
from Utils import parse_results,change_weights

def main():
    if ( len ( sys.argv ) <= 1 ) :
        print "Arguments needed: config_file <start_date end_date>"
        sys.exit(0)

    # Get handle of config file
    _config_file = sys.argv[1]
    _config = ConfigParser.ConfigParser()
    _config.readfp( open( _config_file, 'r' ) )

    # Read product list from config file
    _trade_products = _config.get( 'Products', 'trade_products' ).strip().split(",")

    if len ( sys.argv ) == 4 :
        _start_date = sys.argv[2]
        _end_date = sys.argv[3]
    else :
        _start_date = _config.get( 'Dates', 'start_date' )
        _end_date = _config.get( 'Dates', 'end_date' )

    #Initialize weights to be equal
    _w = np.ones(len(_trade_products))/float(len(_trade_products))

    def rosen(_weights):
        change_weights( _config_file, _trade_products, _weights )
        # Run the strategy on the input weights
        proc = subprocess.Popen(['python', 'Simulator.py', _config_file, _start_date, _end_date ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        _results = parse_results( proc.communicate()[0] )
        return ( -_results['Sharpe Ratio'] )

    _final_weights = minimize ( rosen, _w, method='nelder-mead',callback=change_weights).x
    print 'FINAL WEIGHTS: ',_final_weights

if __name__ == '__main__':
    main()
