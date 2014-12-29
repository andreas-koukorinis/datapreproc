#!/usr/bin/env python

import os
import sys
import pandas as pd
import numpy as np
from adjust_split import adjust_for_splits
from backward_dividend_adjust import backward_adjust_dividends

def backfill(prod_backfill, prod, product_type):
    path = '/home/debi/backfill/'
    output_path = '/home/debi/backfill/'
    columns = ['date','open','high','low','close','volume','dividend']

    adjust_for_splits([prod], product_type)
    backward_adjust_dividends([prod], product_type)
    
    prod_file = path+prod+'_backward_dividend_adjusted.csv'
    df_prod = pd.read_csv(prod_file)

    prod_backfill_file = path+prod_backfill[0]+'/'+prod_backfill+'.csv'
    df_prod_bf = pd.read_csv(prod_backfill_file,names=columns)

    starting_date = df_prod_bf['date'].iloc[0] 
    starting_index = df_prod[df_prod['date']==starting_date].index[0] #TODO add closest date 

    #TODO print gap period in between them
    
    df_backfilled = pd.DataFrame(data=df_prod_bf.head(1))
    n = 1
    for i in xrange(starting_index-1,-1,-1):
        backfill_ratio = df_prod['backward_adjusted_close'].iloc[i]/df_prod['backward_adjusted_close'].iloc[i+1]
        df_backfilled.loc[n,'date'] = df_prod.loc[i,'date']
        
        df_backfilled.loc[n,'open'] = df_backfilled.loc[n-1,'open'] * backfill_ratio
        df_backfilled.loc[n,'high'] = df_backfilled.loc[n-1,'high'] * backfill_ratio
        df_backfilled.loc[n,'low'] = df_backfilled.loc[n-1,'low'] * backfill_ratio
        df_backfilled.loc[n,'close'] = df_backfilled.loc[n-1,'close'] * backfill_ratio
        
        n += 1

    df_backfilled['volume'] = 0 # because what we might use may be a fund and have no volume
    df_backfilled['dividend'] = 0
    
    df_prod_bf = pd.concat([df_backfilled.sort(['date'])[:-1],df_prod_bf])
    df_prod_bf.to_csv(output_path+prod_backfill+'_backfilled'+'.csv',index=False)
    


def __main__() :
    if len( sys.argv ) > 1:
        product_type = sys.argv[1]
        prod = sys.argv[2]
        prod_backfill = sys.argv[3]
    else:
        print 'python backfill.py etf/fund product product_to_be_backfilled'
        sys.exit(0)
    
    backfill(prod_backfill, prod, product_type)    

if __name__ == '__main__':
    __main__();
