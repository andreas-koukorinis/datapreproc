#!/usr/bin/env python

import os
import sys
import pandas as pd
from datetime import datetime

def process_for_dump( products, product_type ):
    path = '/home/cvdev/stratdev/DataCleaning/'
    prices_files = [ path+product+'_forward_dividend_adjusted.csv' for product in products ]
    dateparse = lambda x: datetime.strptime(x, '%Y%m%d').date()

    for i in range(len(products)):
        if product_type == 'ETF': # If the product type is ETF
            df = pd.read_csv(prices_files[i],parse_dates=['date'], date_parser=dateparse,names=['date','open','high','low','close','volume','dividend','backward_adjusted_close','forward_adjusted_close']) # Load as dataframe and name columns        
            df['product'] = product #Add product column
            df = df[[ 'date','product','open','high','low','close','backward_adjusted_close','forward_adjusted_close','volume','dividend']]
        elif product_type == 'MF': # If the product type is MUTUAL FUND
            df = pd.read_csv(prices_files[i],parse_dates=['date'], date_parser=dateparse,names=['date','close','dividend','capital_gain','backward_adjusted_close','forward_adjusted_close']) # Load as dataframe and name columns 
            df['product'] = product #Add product column
            df = df[[ 'date','product','close','backward_adjusted_close','forward_adjusted_close','dividend']]
        df.to_csv(product+'.csv',index=False) # Save result to csv 

def __main__() :
    if len( sys.argv ) > 1:
        product_type = sys.argv[1]
        products = []
        for i in range(2,len(sys.argv)):
            products.append(sys.argv[i])
    else:
        print 'python process_db.py ETF/MF product1 product2 product3 .. productn'
        sys.exit(0)
    process_for_dump( products, product_type )    

if __name__ == '__main__':
    __main__();
