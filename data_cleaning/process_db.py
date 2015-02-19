# cython: profile=True
#!/usr/bin/env python

import os
import sys
import pandas as pd
from datetime import datetime

def process_for_dump(path, products, product_type ):
    dateparse = lambda x: datetime.strptime(x, '%Y%m%d').date() # Parse dates in format required by mysql i.e. YYYY-MM-DD

    for product in products:
        prices_file = path+product+'_forward_dividend_adjusted.csv'
        df = pd.read_csv(prices_file,parse_dates =['date'],header=0,date_parser=dateparse)
        df['product'] = product
        if product_type == 'etf': # If the product type is ETF
            df = df[[ 'date','product','open','high','low','close','backward_adjusted_close','forward_adjusted_close','volume','dividend']] # Rearrange columns to db-table format
            df['volume'] *= 100 # Volume is specified in 100's by csi
        elif product_type == 'fund': # If the product type is MUTUAL FUND
            df = df[[ 'date','product','close','asking_price','backward_adjusted_close','forward_adjusted_close','dividend','capital_gain']] # Rearrange columns to db-table format
        df.to_csv(path+product+'.csv',index=False) # Save result to csv 

def __main__() :
    if len( sys.argv ) > 1:
        product_type = sys.argv[2]
        path = sys.argv[1].replace('~', os.path.expanduser('~'))
        products = []
        for i in range(3,len(sys.argv)):
            products.append(sys.argv[i])
    else:
        print 'python process_db.py path etf/fund product1 product2 product3 .. productn'
        sys.exit(0)
    process_for_dump( path, products, product_type )    

if __name__ == '__main__':
    __main__();
