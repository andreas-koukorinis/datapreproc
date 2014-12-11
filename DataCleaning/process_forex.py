#!/usr/bin/env python

import os
import sys
import pandas as pd
from datetime import datetime

#product -> (filename, to_invert)
product_info = {'JPYUSD': ('US$/US$_XX46.csv', True), 'CADUSD' : ('US$/US$_XX39.csv', True), 'GBPUSD' : ('GB2/GB2_XX60.csv', False), 'EURUSD' : ('EU2/EU2_XX60.csv', False) }

def process_for_dump(products):
    in_path = '/apps/data/csi/forex/'
    out_path = 'Data/'
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    dateparse = lambda x: datetime.strptime(x, '%Y%m%d').date() # Parse dates in format required by mysql i.e. YYYY-MM-DD

    for product in products:
        prices_file = in_path + product_info[product][0]
        df = pd.read_csv(prices_file,parse_dates =['date'],names=['date', 'open', 'old_high', 'old_low', 'close', 'a', 'b', 'c', 'd'], date_parser=dateparse)
        df['product'] = product
        if product_info[product][1]: # if we need to invert the fx pair
            df['open'] = 1.0/df['open']
            df['close'] = 1.0/df['close']
            df['high'] = 1.0/df['old_low']
            df['low'] = 1.0/df['old_high']
        else:
            df['high'] = df['old_high']
            df['low'] = df['old_low']
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
