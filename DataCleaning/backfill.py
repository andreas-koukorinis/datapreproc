#!/usr/bin/env python

import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from adjust_split import adjust_for_splits
from backward_dividend_adjust import backward_adjust_dividends

def backfill(prod_backfill, prod, product_type, plot_option):
    """
    Backfills open, close, high and low prices for ETFs which were created recently.
    Given input asset which is available when the ETF isn't it backfills the prices.
    First, it adjusts for splits in the input file and then backward adjusts for dividends.
    Now the backward adjusted close price is used to backfill to keep daily log returns equal
    for both the assets.

    If the assets are A and B and price of A when unavailable is being backfilled using price of B
    A_close_price_backfill_i/A_close_price_backfill_(i+1) = B_backward_adjusted_close_i/ B_backward_adjusted_close_(i+1)
    Asset prices of A where available are used without modification.

    Args:
        prod_backfill(string): Product which is being _backfilled
        prod(string): Product being used to backfill
        product_type(string): Type(etf/fund) of prod. Backward adjusted pricing will difer based on this.

    Returns:
        Nothing.

    Outputs:
        In output_path, it creates a new file prod_backfill+'_backfilled.csv' which contains all the prices for
        the product being backfilled.
    """
    path = '/home/debi/backfill/'
    output_path = '/home/debi/backfill/'
    columns = ['date','open','high','low','close','volume','dividend']

    # Adjust for splits and create backward dividend adjusd file
    adjust_for_splits([prod], product_type)
    backward_adjust_dividends([prod], product_type)
    
    # Read data from CSVs to datafames for both products
    prod_file = path+prod+'_backward_dividend_adjusted.csv'
    df_prod = pd.read_csv(prod_file)
    prod_backfill_file = path+prod_backfill[0]+'/'+prod_backfill+'.csv'
    df_prod_bf = pd.read_csv(prod_backfill_file,names=columns)

    # Get last date available in df_prod_bf
    # This is the starting date for backfill
    starting_date = df_prod_bf['date'].iloc[0]
    # Get list of all dates after which data is available in df_prod
    # Select the index of closest date to starting date as starting index
    starting_index = df_prod[df_prod['date']>=starting_date].index[0]

    #Print gap period in between them
    print "Gap period:" + str(starting_date - df_prod['date'].iloc[starting_index])
    
    # Add last last available enry in datafame to new dataframe 
    # This dataframe will store all backfilled  values
    df_backfilled = pd.DataFrame(data=df_prod_bf.head(1))
    # Keeps track index in new backfilled dataframe
    n = 1
    
    for i in xrange(starting_index-1,-1,-1):
        # backfill_ratio is kept to ensure daily log returns is same between both poducts when data is unavailable
        backfill_ratio = df_prod['backward_adjusted_close'].iloc[i]/df_prod['backward_adjusted_close'].iloc[i+1]
        df_backfilled.loc[n,'date'] = df_prod.loc[i,'date']
        # A_price_backfill_i/A_price_backfill_(i+1) = B_backward_adjusted_close_i/ B_backward_adjusted_close_(i+1)        
        df_backfilled.loc[n,'open'] = df_backfilled.loc[n-1,'open'] * backfill_ratio
        df_backfilled.loc[n,'high'] = df_backfilled.loc[n-1,'high'] * backfill_ratio
        df_backfilled.loc[n,'low'] = df_backfilled.loc[n-1,'low'] * backfill_ratio
        df_backfilled.loc[n,'close'] = df_backfilled.loc[n-1,'close'] * backfill_ratio
        
        n += 1

    df_backfilled['volume'] = 0  # because what we might use may be a fund and have no volume
    df_backfilled['dividend'] = 0  # assume no dividends are being given out as backwad adjusted price used already 
    
    # Concat the backfilled dataframe and the available dataframe for prod_bf
    df_prod_bf = pd.concat([df_backfilled.iloc[::-1][:-1],df_prod_bf])
    # Output to file
    df_prod_bf.to_csv(output_path+prod_backfill+'_backfilled'+'.csv',index=False)

    #df_prod_bf = pd.read_csv(output_path+prod_backfill+'_backfilled'+'.csv')
    if plot_option.lower() == 'y':
        plot_path = '/home/debi/backfill/'
        ax = df_prod.plot(y='backward_adjusted_close',use_index=False,legend=False)
        patches1, labels1 = ax.get_legend_handles_labels()
        ax2 = df_prod_bf.plot(y='close',ax=ax,use_index=False,secondary_y=True,legend=False)
        patches2, labels2 = ax2.get_legend_handles_labels()
        my_labels = [prod,prod_backfill+"(Right)"]
        ax.legend(patches1 + patches2,my_labels,loc='best')
        ax.axvline(x=starting_index)
        plt.savefig(output_path+"plot_"+prod_backfill+"_"+prod+".png")


def __main__() :
    if len( sys.argv ) > 1:
        product_type = sys.argv[1]
        prod = sys.argv[2]
        prod_backfill = sys.argv[3]
        plot_option = sys.argv[4]
    else:
        print 'python backfill.py etf/fund product product_to_be_backfilled plot(y/n)'
        sys.exit(0)
    
    backfill(prod_backfill, prod, product_type, plot_option)    

if __name__ == '__main__':
    __main__();
