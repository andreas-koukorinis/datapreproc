# cython: profile=True
#!/usr/bin/env python

import os
import sys
import pandas as pd

def adjust_for_splits( path, products, product_type, output_path=None):
    if output_path == None:
        output_path = path
    for product in products:
        if os.path.isfile(output_path+product+'_split_adjusted.csv'):
            continue
        prices_file = path+product[0]+'/'+product+'.csv'
        split_file = path+product[0]+'/'+product+'.SPT'
        if product_type == 'etf':
            df1 = pd.read_csv(prices_file,names=['date','open','high','low','close','volume','dividend'])
            if not os.path.isfile(split_file):
                df1.to_csv(output_path+product+'_split_adjusted'+'.csv',index=False)
                continue
            df2 = pd.read_csv(split_file,names=['date','new','old'])
            split_factor = 1.0
            for index, row in df2.iterrows():
                split_factor = split_factor*(float(row['new'])/float(row['old']))
                df1.loc[ (df1.date < row['date']) ,'close'] /=split_factor
                df1.loc[ (df1.date < row['date']) ,'high'] /=split_factor
                df1.loc[ (df1.date < row['date']) ,'low'] /=split_factor
                df1.loc[ (df1.date < row['date']) ,'open'] /=split_factor
                df1.loc[ (df1.date < row['date']) ,'volume'] *=split_factor
                df1.loc[ (df1.date < row['date']) ,'dividend'] /=split_factor
            df1.to_csv(output_path+product+'_split_adjusted'+'.csv',index=False)
        elif product_type == 'fund':
            df1 = pd.read_csv(prices_file,names=['date','close','asking_price','close2','close3','dividend','capital_gain'])
            df = df1[['date','close','asking_price','dividend','capital_gain']]
            if not os.path.isfile(split_file):
                df.to_csv(output_path+product+'_split_adjusted'+'.csv',index=False)
                continue
            df2 = pd.read_csv(split_file,names=['date','new','old'])
            split_factor = 1.0
            for index, row in df2.iterrows():
                split_factor = split_factor*(row['new']/row['old'])
                df.loc[ (df.date < row['date']) ,'close'] /=split_factor
                df.loc[ (df.date < row['date']) ,'asking_price'] /=split_factor
                df.loc[ (df.date < row['date']) ,'dividend'] /=split_factor
                df.loc[ (df.date < row['date']) ,'capital_gain'] /=split_factor
            df.to_csv(output_path+product+'_split_adjusted'+'.csv',index=False)

def __main__() :
    if len( sys.argv ) > 1:
        product_type = sys.argv[3]
        path = sys.argv[2].replace('~', os.path.expanduser('~')) 
        outputh_path = sys.argv[1].replace('~', os.path.expanduser('~')) 
        products = []
        for i in range(4,len(sys.argv)):
            products.append(sys.argv[i])
    else:
        print 'python adjust_split.py output_path path etf/fund product1 product2 product3 .. productn'
        sys.exit(0)
    adjust_for_splits(path, products, product_type, output_path)    

if __name__ == '__main__':
    __main__();
