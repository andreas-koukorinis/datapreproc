#!/usr/bin/env python

import os
import sys
import pandas as pd
from datetime import datetime

product_filename = {'JPYUSD':'US$/US$_XX46.csv', 'CADUSD':'US$/US$_XX39.csv', 'GBPUSD':'US$/US$_XX55.csv', 'EURUSD':'EU2/EU2_XX60.csv'}

def process_for_dump(products):
    in_path = '/apps/data/csi/forex/'
    out_path = 'Data/'
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    dateparse = lambda x: datetime.strptime(x, '%Y%m%d').date() # Parse dates in format required by mysql i.e. YYYY-MM-DD

    for product in products:
        prices_file = in_path + product_filename[product]
        df = pd.read_csv(prices_file,parse_dates =['date'],names=['date', 'open', 'high', 'low', 'close', 'a', 'b', 'c', 'd'], date_parser=dateparse)
        df['product'] = product
        if product == 'EURUSD':
            df['open'] = 1.0/df['open']
            df['high'] = 1.0/df['high']
            df['low'] = 1.0/df['low']
            df['close'] = 1.0/df['close']
        df = df[[ 'date','product','open','high','low','close']] # Rearrange columns to db-table format
        df.to_csv(out_path + product + '.csv', index=False) # Save result to csv 

def __main__() :
    if len( sys.argv ) > 1:
        products = []
        for i in range(1,len(sys.argv)):
            products.append(sys.argv[i])
    else:
        print 'python process_forex.py product1 product2 product3 .. productn'
        sys.exit(0)
    process_for_dump(products)

if __name__ == '__main__':
    __main__();
