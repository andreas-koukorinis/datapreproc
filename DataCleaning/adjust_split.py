#!/usr/bin/env python

import os
import sys
import pandas as pd

def adjust_for_splits( products, product_type ):
    path = '/home/cvdev/data/csi/CVHISTORICAL_STOCKS/'
    prices_files = [ path+product[0]+'/'+product+'.csv' for product in products ]
    split_files = [ path+product[0]+'/'+product+'.SPT' for product in products ]
    for i in range(len(products)):
        if product_type == 'ETF':
            df1 = pd.read_csv(prices_files[i],names=['date','open','high','low','close','volume','dividends'])
            if not os.path.isfile(split_files[i]):
                df1.to_csv(product+'_split_adjusted'+'.csv',index=False,header=False)
                continue
            df2 = pd.read_csv(split_files[i],names=['date','new','old'])
            split_factor = 1.0
            for index, row in df2.iterrows():
                split_factor = split_factor*(row['new']/row['old'])
                df1.loc[ (df1.date < row['date']) ,'close'] /=split_factor
                df1.loc[ (df1.date < row['date']) ,'high'] /=split_factor
                df1.loc[ (df1.date < row['date']) ,'low'] /=split_factor
                df1.loc[ (df1.date < row['date']) ,'open'] /=split_factor
                df1.loc[ (df1.date < row['date']) ,'volume'] *=split_factor
            df1.to_csv(product+'_split_adjusted'+'.csv',index=False,header=False)
        elif product_type == 'MF':
            df1 = pd.read_csv(prices_files[i],names=['date','close','close1','close2','close3','dividends','capitalgain'])
            df = df1[['date','close','dividends','capitalgain']]
            if not os.path.isfile(split_files[i]):
                df.to_csv(product+'_split_adjusted'+'.csv',index=False,header=False)
                continue
            df2 = pd.read_csv(split_files[i],names=['date','new','old'])
            split_factor = 1.0
            for index, row in df2.iterrows():
                split_factor = split_factor*(row['new']/row['old'])
                df.loc[ (df.date < row['date']) ,'close'] /=split_factor
            df.to_csv(product+'_split_adjusted'+'.csv',index=False,header=False)

def __main__() :
    if len( sys.argv ) > 1:
        product_type = sys.argv[1]
        products = []
        for i in range(2,len(sys.argv)):
            products.append(sys.argv[i])
    else:
        print 'python adjust_split.py ETF/MF product1 product2 product3 .. productn'
        sys.exit(0)
    adjust_for_splits( products, product_type )    

if __name__ == '__main__':
    __main__();
