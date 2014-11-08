#!/usr/bin/env python

import os
import sys
import pandas as pd

def backward_adjust_dividends( products, product_type ):
    path = '/home/cvdev/stratdev/DataCleaning/'
    prices_files = [ path+product+'_split_adjusted.csv' for product in products ]

    for i in range(len(products)):
        if product_type == 'ETF':# If the product type is ETF
            df = pd.read_csv(prices_files[i],names=['date','open','high','low','close','volume','dividends']) # Load as dataframe and name columns
            df1 = df[ df.dividends + df.capitalgain > 0.0 ] # Select rows from dataframe in which the dividend or capital gain was paid
            #print df1
            df['backward_adjusted_close'] = df['close'] # Make a new column and copy close prices initially
            for index, row in df1.iterrows(): # For each of the payouts
                dividend_factor =  1 +  row['dividends'] / row['close'] # Calculate the dividend factor
                df.loc[ (df.date < row['date']) ,'backward_adjusted_close'] /= dividend_factor # Divide all prices earlier to this payout by the dividend factor
            df['backward_adjusted_close'] = df['backward_adjusted_close'].round(2) # Round to 2 decimal places
            df.to_csv(product+'_dividend_adjusted'+'.csv',index=False,header=False) # Save result to csv           

        elif product_type == 'MF':
            df = pd.read_csv(prices_files[i],names=['date','close','dividends','capitalgain'])
            df1 = df[ df.dividends + df.capitalgain > 0.0 ]
            #print df1
            df['backward_adjusted_close'] = df['close'] # Make a new column and copy close prices initially
            for index, row in df1.iterrows(): # For each of the payouts               
                dividend_factor = 1 + ( row['dividends'] + row['capitalgain'] ) / row['close'] # Calculate the dividend factor
                df.loc[ (df.date < row['date']) ,'backward_adjusted_close'] /= dividend_factor # Divide all prices earlier to this payout by the dividend factor
            df['backward_adjusted_close'] = df['backward_adjusted_close'].round(2) # Round to 2 decimal places
            df.to_csv(product+'_dividend_adjusted'+'.csv',index=False,header=False) # Save result to csv 

def __main__() :
    if len( sys.argv ) > 1:
        product_type = sys.argv[1]
        products = []
        for i in range(2,len(sys.argv)):
            products.append(sys.argv[i])
    else:
        print 'python adjust_split.py ETF/MF product1 product2 product3 .. productn'
        sys.exit(0)
    backward_adjust_dividends( products, product_type )    

if __name__ == '__main__':
    __main__();
