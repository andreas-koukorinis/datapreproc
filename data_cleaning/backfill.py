#!/usr/bin/env python
import argparse
from datetime import date, datetime
import os
import shutil
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from adjust_split import adjust_for_splits
from backward_dividend_adjust import backward_adjust_dividends
from forward_dividend_adjust import forward_adjust_dividends
from calculate import convert_daily_returns_to_yyyymm_monthly_returns_pair, compute_correlation, compute_daily_log_returns

parse = lambda x: datetime.strptime(x, '%Y%m%d')

def get_correlation_monthly_returns(path, df_prod_bf, prod, product_type, output_path):
    # Adjust for splits and create backward dividend adjusted file
    if product_type == 'index':
        columns = ['date','open','high','low','close','volume','dividend']
        prod_file = path+prod[0]+'/'+prod+'.csv'
        df_prod = pd.read_csv(prod_file,names=columns, parse_dates=['date'], date_parser=parse)
        returns = compute_daily_log_returns(df_prod['close'].values)
    else:
        adjust_for_splits(path, [prod], product_type, output_path)
        backward_adjust_dividends(output_path, [prod], product_type)
        # Read data from CSVs to dataframe
        prod_file = output_path+prod+'_backward_dividend_adjusted.csv'
        df_prod = pd.read_csv(prod_file, parse_dates=['date'], date_parser=parse)
        returns = compute_daily_log_returns(df_prod['backward_adjusted_close'].values)
            
    returns_bf = compute_daily_log_returns(df_prod_bf['backward_adjusted_close'].values)
    labels_monthly_returns_prod_bf = convert_daily_returns_to_yyyymm_monthly_returns_pair(df_prod_bf['date'], returns_bf)
    labels_monthly_returns_prod = convert_daily_returns_to_yyyymm_monthly_returns_pair(df_prod['date'], returns)

    return compute_correlation(labels_monthly_returns_prod_bf, labels_monthly_returns_prod)

def backfill_each_product(path, df_prod_bf, prod, product_type, beta_adjust=False, output_path=None):
    """
    If the assets are A and B and price of A when unavailable is being backfilled using price of B
    A_close_price_backfill_i/A_close_price_backfill_(i+1) = B_backward_adjusted_close_i/ B_backward_adjusted_close_(i+1)
    Asset prices of A where available are used without modification.

    Args:
        df_prod_bf(dataframe): Dataframe of product being backfilled
        prod(string): Current products being used to backfill
        product_types(string): Type(etf/fund) of current product. Backward adjusted pricing will difer based on this.

    Returns:
        df_prod_bf(dataframe): backfilled data DataFrame
        starting_index(int): index at which backfilling was started
    """
    if output_path == None:
        output_path = path
    # Adjust for splits and create backward dividend adjusted file
    if not product_type == 'index':
        adjust_for_splits(path, [prod], product_type, output_path)
        backward_adjust_dividends(output_path, [prod], product_type)
        # Read data from CSVs to datarframe
        prod_file = output_path+prod+'_backward_dividend_adjusted.csv'
        df_prod = pd.read_csv(prod_file, parse_dates=['date'], date_parser=parse)
    else:
        columns = ['date','open','high','low','close','volume','dividend']
        prod_file = path+prod[0]+'/'+prod+'.csv'
        df_prod = pd.read_csv(prod_file,names=columns, parse_dates=['date'], date_parser=parse)

    if beta_adjust == True:
        returns_bf = df_prod_bf.close / df_prod_bf.close.shift(1)
        std_prod_bf = np.std(returns_bf.values[1:])
        returns = df_prod.close / df_prod.close.shift(1)
        std_prod = np.std(returns.values[1:])
        beta_factor = std_prod_bf/std_prod
    else:
        beta_factor = 1.0 

    # Get last date available in df_prod_bf
    # This is the starting date for backfill
    starting_date = df_prod_bf['date'].iloc[0]
    # Get list of all dates after which data is available in df_prod
    # Select the index of closest date to starting date as starting index
    starting_index = df_prod[df_prod['date']>=starting_date].index[0]

    #Print gap period in between them
    #print "Gap period:" + str(starting_date - df_prod['date'].iloc[starting_index])
    
    # Add last last available entry in dataframe to new dataframe 
    # This dataframe will store all backfilled  values
    df_backfilled = pd.DataFrame(data=df_prod_bf.head(1).reset_index(drop=True))
    # Keeps track index in new backfilled dataframe
    n = 1

    if not product_type == 'index':
        df_prod['backward_adjusted_close'] = beta_factor*df_prod['backward_adjusted_close']
    else:
        df_prod['close'] = beta_factor * df_prod['close']
    
    for i in xrange(starting_index-1,-1,-1):
        # backfill_ratio is kept to ensure daily log returns is same between both poducts when data is unavailable
        if not product_type == 'index':
            backfill_ratio = df_prod['backward_adjusted_close'].iloc[i]/df_prod['backward_adjusted_close'].iloc[i+1]
        else:
            backfill_ratio = df_prod['close'].iloc[i]/df_prod['close'].iloc[i+1]
        df_backfilled.loc[n,'date'] = df_prod.loc[i,'date']
        # A_price_backfill_i/A_price_backfill_(i+1) = B_backward_adjusted_close_i/ B_backward_adjusted_close_(i+1)        
        df_backfilled.loc[n,'open'] = df_backfilled.loc[n-1,'open'] * backfill_ratio
        df_backfilled.loc[n,'high'] = df_backfilled.loc[n-1,'high'] * backfill_ratio
        df_backfilled.loc[n,'low'] = df_backfilled.loc[n-1,'low'] * backfill_ratio
        df_backfilled.loc[n,'close'] = df_backfilled.loc[n-1,'close'] * backfill_ratio
        n += 1
    
    df_backfilled['open'] = df_backfilled['open']
    df_backfilled['low'] = df_backfilled['low']
    df_backfilled['high'] =  df_backfilled['high']
    df_backfilled['close'] = df_backfilled['close']
    df_backfilled['volume'] = 0  # because what we might use may be a fund and have no volume
    df_backfilled['dividend'] = 0  # assume no dividends are being given out as backwad adjusted price used already 
    
    # Concat the backfilled dataframe and the available dataframe for prod_bf
    df_prod_bf = pd.concat([df_backfilled.iloc[::-1][:-1],df_prod_bf])

    return df_prod_bf, starting_date.to_datetime().strftime("%Y%m%d")

def auto_backfill(path, output_path, prod_backfill, plot_option):
    """
    Methodology for automatic backfilling
    1) Look in folder of assets(funds or indices) whose prices are available till 1995.
    2) Find asset in this folder with maximum correlation in monthly returns.
    3) Use beta-adjusted values of the chosen fund or index to backfill.
    4) If maximum correlation is below a certain threshold don't backfill. This asset will be advised to be backfilled manually.
    """
    if os.path.isfile(output_path+prod_backfill+'_backfilled'+'.csv'):
        print "%s already backfilled"%prod_backfill
        return True
    prod_backfill_file = path+prod_backfill[0]+'/'+prod_backfill+'.csv'
    adjust_for_splits(path, [prod_backfill], 'etf', output_path)
    backward_adjust_dividends(output_path, [prod_backfill], 'etf')
    #df_prod_bf = pd.read_csv(path+prod_backfill+'_split_adjusted.csv')
    df_prod_bf = pd.read_csv(output_path+prod_backfill+'_backward_dividend_adjusted.csv', parse_dates=['date'], date_parser=parse)

    products = ['VTSMX', 'GMHBX', 'VBMFX', 'VEIEX', '^MXEA', '@DJCI', 'VWAHX', 'VFISX', 'VIPSX', 'VSIIX', 'CVK', 'DFSVX', \
                '^XMSC', 'MARFX', 'XMSW', 'VIVAX', 'VGTSX', 'VGSIX', 'VEIEX', 'RUI', 'PEBIX', 'SPX', '@GU6']
    products_type = ['fund', 'fund', 'fund', 'fund', 'index', 'index', 'fund', 'fund', 'fund', 'fund', 'index', 'fund', \
                 'index', 'fund', 'index', 'fund', 'fund', 'fund', 'fund', 'index', 'fund', 'index', 'index']
    correlations = []
    for i in xrange(len(products)):
        correlations.append(get_correlation_monthly_returns(path, df_prod_bf, products[i], products_type[i], output_path))
    
    #print zip(products, correlations)
    if max(correlations) < 0.7:
        print ('Product %s not highly correlated with any of our proxy products. Please backfill manually.'%prod_backfill)
        return False

    backfill_choice = correlations.index(max(correlations))
    print "%s being backfilled with %s. Correlation between them is: %0.3f" % (prod_backfill, products[backfill_choice], max(correlations)) ,
    df_prod_bf, starting_date = backfill_each_product(path, df_prod_bf, products[backfill_choice], products_type[backfill_choice], beta_adjust=True, output_path=output_path)
    print " from %s" % (starting_date)
    df_prod_bf['date'] = df_prod_bf['date'].apply(lambda x: x.strftime('%Y%m%d'))

    df_prod_bf.to_csv(output_path+prod_backfill+'_backfilled'+'.csv',index=False)
    
    if plot_option.lower() == 'y':
        df_prod_bf = pd.read_csv(output_path+prod_backfill+'_backfilled'+'.csv')
        ax = df_prod_bf.plot(y='close',use_index=False)
        starting_index = df_prod_bf[df_prod_bf['date']==int(starting_date)].index[0]
        ax.axvline(x=starting_index)
        plt.savefig(output_path+"plot_"+prod_backfill+".png")
    
    return True 
    
def backfill(path, output_path, prod_backfill, prod_list, product_types, plot_option):
    """
    Backfills open, close, high and low prices for ETFs which were created recently.
    Given input asset which is available when the ETF isn't it backfills the prices.
    First, it adjusts for splits in the input file and then backward adjusts for dividends.
    Now the backward adjusted close price is used to backfill to keep daily log returns equal
    for both the assets.

    Args:
        prod_backfill(string): Product which is being _backfilled
        prod_list(list of strings): List of products being used to backfill
        product_types(list of string): List of Type(etf/fund) of products. Backward adjusted pricing will difer based on this.
        plot_option: y/n option to plot backfilled pricces

    Returns:
        Nothing.

    Outputs:
        In output_path, it creates a new file prod_backfill+'_backfilled.csv' which contains all the prices for
        the product being backfilled.
    """       
    # Read data from file
    prod_backfill_file = path+prod_backfill[0]+'/'+prod_backfill+'.csv'
    adjust_for_splits(path, [prod_backfill], 'etf', output_path)
    df_prod_bf = pd.read_csv(output_path+prod_backfill+'_split_adjusted.csv', parse_dates=['date'], date_parser=parse)
    
    starting_dates = []
    for prod in zip(prod_list,product_types):
        df_prod_bf,temp_dt = backfill_each_product(path, df_prod_bf, prod[0], prod[1], output_path=output_path)
        starting_dates.append(temp_dt)

    # Output to file
    df_prod_bf['date'] = df_prod_bf['date'].apply(lambda x: x.strftime('%Y%m%d'))
    df_prod_bf.to_csv(output_path+prod_backfill+'_backfilled'+'.csv',index=False)
        
    if plot_option.lower() == 'y':
        df_prod_bf = pd.read_csv(output_path+prod_backfill+'_backfilled'+'.csv')
        # ax = df_prod.plot(y='backward_adjusted_close',use_index=False,legend=False)
        # patches1, labels1 = ax.get_legend_handles_labels()
        # ax2 = df_prod_bf.plot(y='close',ax=ax,use_index=False,secondary_y=True,legend=False)
        ax = df_prod_bf.plot(y='close',use_index=False)
        for starting_date in starting_dates:
            # prod_file = path+prod+'_backward_dividend_adjusted.csv'
            # df_prod = pd.read_csv(prod_file)
            # ax2 = df_prod.plot(y='close',ax=ax,use_index=False,secondary_y=True)
            # patches2, labels2 = ax2.get_legend_handles_labels()
            # my_labels = [prod,prod_backfill+"(Right)"]
            # ax.legend(patches1 + patches2,my_labels,loc='best')
            starting_index = df_prod_bf[df_prod_bf['date']==int(starting_date)].index[0]
            ax.axvline(x=starting_index)
        plt.savefig(output_path+"plot_"+prod_backfill+".png")

def dividend_adjust(path, prod):
    shutil.copy2(path+prod+'_backfilled.csv', path+prod+'_split_adjusted.csv')
    backward_adjust_dividends(path, [prod], ['etf'])
    forward_adjust_dividends(path, [prod], ['etf'], path+'complete/')


def __main__() :
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('output_path')
    parser.add_argument('prod_backfill')
    parser.add_argument('-a', type=str, help='Auto-backfill', default='y', dest='auto_option')
    parser.add_argument('-p', type=str, help='Plot after backfill', default='n', dest='plot_option')
    parser.add_argument('-d', type=str, help='Dividend adjust after backfill', default='y', dest='dividend_adjust')
    parser.add_argument('--prods', nargs='+', help='etf/fund/index1 product1 etf/fund/index2 product2...', default="", dest="prod_list")
    args = parser.parse_args()
    products = []
    product_types = []
    for i in range(0,len(args.prod_list),2):
        product_types.append(args.prod_list[i])
        products.append(args.prod_list[i+1])
    
    if args.auto_option == 'y':
        backfilled = auto_backfill(args.path, args.output_path, args.prod_backfill, args.plot_option)
    else:
        backfill(args.path, args.output_path, args.prod_backfill, products, product_types, args.plot_option)

    if args.dividend_adjust == 'y' and backfilled == True:
        dividend_adjust(args.output_path, args.prod_backfill)

if __name__ == '__main__':
    __main__();
