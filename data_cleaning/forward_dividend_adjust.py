#!/usr/bin/env python

import os
import sys
import pandas as pd

def forward_adjust_dividends( products, product_type ):
    path = '/home/cvdev/stratdev/data_cleaning/data/'
    for product in products:
        prices_file = path+product+'_backward_dividend_adjusted.csv'
        df = pd.read_csv(prices_file,header=0)
        if product_type == 'etf': # If the product type is ETF
            df1 = df[ df.dividend > 0.0 ] # Select rows from dataframe in which the dividend was paid
            df['forward_adjusted_close'] = df['close'] # Make a new column and copy close prices initially
            for index, row in df1.iterrows(): # For each of the payouts
                dividend_factor =  1 +  row['dividend'] / row['close'] # Calculate the dividend factor
                df.loc[ (df.date >= row['date']) ,'forward_adjusted_close'] *= dividend_factor # Multiply all prices after(including) this payout by the dividend factor

        elif product_type == 'fund': # If the product type is MUTUAL FUND
            df1 = df[ df.dividend + df.capital_gain > 0.0 ] # Select rows from dataframe in which the dividend or capital gain was paid
            df['forward_adjusted_close'] = df['close'] # Make a new column and copy close prices initially
            for index, row in df1.iterrows(): # For each of the payouts               
                dividend_factor = 1 + ( row['dividend'] + row['capital_gain'] ) / row['close'] # Calculate the dividend factor
                df.loc[ (df.date >= row['date']) ,'forward_adjusted_close'] *= dividend_factor # Multiply all prices after(including) this payout by the dividend factor
        df.to_csv(path+product+'_forward_dividend_adjusted'+'.csv',index=False) # Save result to csv 

def __main__() :
    if len( sys.argv ) > 1:
        product_type = sys.argv[1]
        products = []
        for i in range(2,len(sys.argv)):
            products.append(sys.argv[i])
    else:
        print 'python adjust_split.py etf/fund product1 product2 product3 .. productn'
        sys.exit(0)
    forward_adjust_dividends( products, product_type )    

if __name__ == '__main__':
    __main__();