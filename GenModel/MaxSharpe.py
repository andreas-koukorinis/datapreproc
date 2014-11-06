#!/usr/bin/env python
import sys
import pandas as pd
import numpy as np
from sklearn import linear_model

def main():
    if ( len ( sys.argv ) <= 2 ) :
        print "Arguments needed: targetrisk returns-data-file weights-file_to_write"
        sys.exit(0)
    _target_risk = float(sys.argv[1])
    _returns_data_filename = sys.argv[2]
    _weights_filename = sys.argv[3]
    df = pd.DataFrame.from_csv(_returns_data_filename)
    _logret_matrix = df.values

    # now add the assuption that the sharpe of all products is the same
    # calcualte observed sharpe
    _shps = np.mean( _logret_matrix, axis=0 )/np.std( _logret_matrix, axis=0 )
    # calculate values to add to the columns to make the sharpe-ratios the same
    _means_to_add = (np.median(_shps) - _shps)*np.std(_logret_matrix, axis=0)
    # after adding this the observed sharpe ratios should be the same
    _equal_sharpe_logret = _logret_matrix + _means_to_add
    _u1 = (_equal_sharpe_logret[:,0]*0 + 1)
    regr = linear_model.LinearRegression ( fit_intercept=False )
    _fit = regr.fit ( _equal_sharpe_logret, _u1 )
    _w = _fit.coef_
    _w = _w / sum ( abs( _w ) )

    #Scale weights to achieve the target risk
    _cov_mat = np.cov( _logret_matrix.T )    
    _annualized_stddev_of_portfolio = 100.0*( np.exp( np.sqrt( 252.0 * (np.asmatrix( _w )*np.asmatrix( _cov_mat )*np.asmatrix( _w ).T) )[0,0]) - 1 )
    _w_new = _w*(_target_risk/_annualized_stddev_of_portfolio)

    #Output and save weights to file
    symbols=df.columns
    s=''
    for symbol,weight in zip(symbols,_w_new):
        s = s + symbol.split('.')[1] + ',' + str(weight) + ' '
    print s
    print 'LEVERAGE: %0.2f'%sum( abs( _w_new ) )        

    with open(_weights_filename,'w') as f:
        f.write( s )    
    #np.savetxt(_weights_filename, s )

if __name__ == '__main__':
    main();
