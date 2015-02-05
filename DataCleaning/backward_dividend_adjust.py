# cython: profile=True
#!/usr/bin/env python

import os
import sys
import pandas as pd

def backward_adjust_dividends( products, product_type ):
    path = '/home/deedee/backfill/stratdev/DataCleaning/Data/'
    for product in products:
        prices_file = path+product+'_split_adjusted.csv'
        df = pd.read_csv(prices_file,header=0)
        if product_type == 'etf':# If the product type is ETF
            df1 = df[ df.dividend > 0.0 ] # Select rows from dataframe in which the dividend was paid
            df['backward_adjusted_close'] = df['close'] # Make a new column and copy close prices initially
            for index, row in df1.iterrows(): # For each of the payouts
                dividend_factor =  1 +  row['dividend'] / row['close'] # Calculate the dividend factor
                df.loc[ (df.date < row['date']) ,'backward_adjusted_close'] /= dividend_factor # Divide all prices earlier to this payout by the dividend factor

        elif product_type == 'fund': # If the product type is MUTUAL FUND
            df1 = df[ df.dividend + df.capital_gain > 0.0 ] # Select rows from dataframe in which the dividend or capital gain was paid
            df['backward_adjusted_close'] = df['close'] # Make a new column and copy close prices initially
            for index, row in df1.iterrows(): # For each of the payouts               
                dividend_factor = 1 + ( row['dividend'] + row['capital_gain'] ) / row['close'] # Calculate the dividend factor
                df.loc[ (df.date < row['date']) ,'backward_adjusted_close'] /= dividend_factor # Divide all prices earlier to this payout by the dividend factor
<<<<<<< HEAD
=======
                                
>>>>>>> 743ca1018744f2ed1ed0dd79c25e54448a55ffb4
        df.to_csv(path+product+'_backward_dividend_adjusted'+'.csv',index=False) # Save result to csv 

def __main__() :
    if len( sys.argv ) > 1:
        product_type = sys.argv[1]
        products = []
        for i in range(2,len(sys.argv)):
            products.append(sys.argv[i])
    else:
        print 'python adjust_split.py etf/fund product1 product2 product3 .. productn'
        sys.exit(0)
    backward_adjust_dividends( products, product_type )    

if __name__ == '__main__':
    __main__();
# #!/usr/bin/env python

# import os
# import sys
# import pandas as pd

# def backward_adjust_dividends( products, product_type ):
#     path = '/home/debi/backfill/'
#     for product in products:
#         prices_file = path+product+'_split_adjusted.csv'
#         df = pd.read_csv(prices_file,header=0)

#         if product_type == 'etf':# If the product type is ETF
#             df1 = df[ df.dividend > 0.0 ] # Select rows from dataframe in which the dividend was paid
#             df['backward_adjusted_close'] = df['close'] # Make a new column and copy close prices initially
#             df['backward_adjusted_open'] = df['open'] # Make a new column and copy open prices initially
#             for index, row in df1.iterrows(): # For each of the payouts
#                 dividend_factor =  1 +  row['dividend'] / row['close'] # Calculate the dividend factor
#                 df.loc[ (df.date < row['date']) ,'backward_adjusted_close'] /= dividend_factor # Divide all prices earlier to this payout by the dividend factor
#                 df.loc[ (df.date < row['date']) ,'backward_adjusted_open'] /= dividend_factor # Divide all prices earlier to this payout by the dividend factor

#         elif product_type == 'fund': # If the product type is MUTUAL FUND
#             df1 = df[ df.dividend + df.capital_gain > 0.0 ] # Select rows from dataframe in which the dividend or capital gain was paid
#             df['backward_adjusted_close'] = df['close'] # Make a new column and copy close prices initially
#             df['backward_adjusted_open'] = df['open'] # Make a new column and copy open prices initially
#             for index, row in df1.iterrows(): # For each of the payouts               
#                 dividend_factor = 1 + ( row['dividend'] + row['capital_gain'] ) / row['close'] # Calculate the dividend factor
#                 df.loc[ (df.date < row['date']) ,'backward_adjusted_close'] /= dividend_factor # Divide all prices earlier to this payout by the dividend factor
#                 df.loc[ (df.date < row['date']) ,'backward_adjusted_open'] /= dividend_factor # Divide all prices earlier to this payout by the dividend factor
                
#         df.to_csv(path+product+'_backward_dividend_adjusted'+'.csv',index=False) # Save result to csv 

# def __main__() :
#     if len( sys.argv ) > 1:
#         product_type = sys.argv[1]
#         products = []
#         for i in range(2,len(sys.argv)):
#             products.append(sys.argv[i])
#     else:
#         print 'python adjust_split.py etf/fund product1 product2 product3 .. productn'
#         sys.exit(0)
#     backward_adjust_dividends( products, product_type )    

# if __name__ == '__main__':
#     __main__();
