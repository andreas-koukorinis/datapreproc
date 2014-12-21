#!/usr/bin/env python
import os
import sys
import subprocess
import commands
import pandas as pd
from datetime import datetime,date,timedelta

def get_specific_ticker_from_filename(filename, to_name):
    pure_filename = os.path.splitext(filename)[0].split('/')[-1]
    yymm = pure_filename.rsplit('_',1)[1]
    yy,mm = yymm[0:2],yymm[2:4]
    month_codes = {'01':'F','02':'G','03':'H','04':'J','05':'K','06':'M','07':'N','08':'Q','09':'U','10':'V','11':'X','12':'Z'}
    return to_name + month_codes[mm] + yy

def convert_to_year_month(YYMM):
    yy = int(YYMM[0:2])
    mm = int(YYMM[2:4])
    if yy >50:
        year = 1900 + yy
    else:
        year = 2000 + yy
    return year*100 + mm

def compare_ticker(ticker1, ticker2):
    month_codes_rev = {'F':'01','G':'02','H':'03','J':'04','K':'05','M':'06','N':'07','Q':'08','U':'09','V':'10','X':'11','Z':'12'}
    code1 = convert_to_year_month(ticker1[-2:] + month_codes_rev[ticker1[-3]])
    code2 = convert_to_year_month(ticker2[-2:] + month_codes_rev[ticker2[-3]])
    return code1 > code2

def check(df1, df2):
    for i in range(len(df1)-2):
        if compare_ticker(df1.loc[i, 'specific_ticker'], df1.loc[i+1, 'specific_ticker']):
            print 'error,%d,1'%i
    for i in range(len(df2)-2):
        if compare_ticker(df2.loc[i, 'specific_ticker'], df2.loc[i+1, 'specific_ticker']):
            print 'error,%d,2'%i

def flips_present(df):
    flips = []
    for i in range(len(df)-2):
        if compare_ticker(df.loc[i, 'specific_ticker'], df.loc[i+1, 'specific_ticker']):
            flips.append(i)
    if len(flips) > 0:
        print 'ERROR: ',flips
        return True
    else:
        print 'No flips found'
        return False

def fix_flips(df):
    pass

def get_second_contract(in_dir, oi_1):
    pass 

def process_futures(product, to_name):
    # Directory for the product
    dir1 = '/apps/data/csi/history_part1/' + product + '/'
    dir2 = '/apps/data/csi/history_part2/' + product + '/'
    if os.path.isdir(dir1):
        in_dir = dir1
    else:
        in_dir = dir2
    out_dir = 'Data/'

    # Remove 0000 file
    _aux_file1 = in_directory + product + '_0000.csv'
    _aux_file2 = in_directory + product + '__0000.csv'
    if os.path.exists(_aux_file1):
        os.remove(_aux_file1)
    if os.path.exists(_aux_file2):
        os.remove(_aux_file2)

    # Get first contract data based on highest open interest
    out_file_aux1 = out_dir + 'aux1_' + product + '.csv'
    cmd = "cd %s;for dd_ in `cat *csv | sort -g | awk -F, '{ print $1 }' | uniq`;do grep $dd_ *csv | sort -k7,7 -rg -t, | head -n1 | sed 's/:/,/' ;done"%(directory)
    output = commands.getoutput(cmd)
    f = open(out_file_aux1, 'w')
    f.write(output)
    f.close()

    # Load the first contract data and check for flips 
    dateparse = lambda x: datetime.strptime(x, '%Y%m%d').date() # Parse dates in format required by mysql i.e. YYYY-MM-DD
    oi_1 = pd.read_csv(out_file_aux1, parse_dates =['date'], date_parser = dateparse, names=['contract_filename', 'date','open','high','low','close','contract_volume','contract_oi','total_volume','total_oi'])
    oi_1['specific_ticker'] = oi_1.apply(lambda row: get_specific_ticker_from_filename(row['contract_filename'], product), axis=1)
    if flips_present(oi_1):
        oi_1 = fix_flips(oi_1)

    # Get the data for second contract based on first contract
    out_file_aux2 = out_dir + 'aux2_' + product + '.csv'
    output = get_second_contract(in_dir, oi_1)
    f = open(out_file_aux2, 'w')
    f.write(output)
    f.close()

    # Process the data and output to db format
    generic_tickers = [product + '_1', product + '_2']
    output_path_1 = out_dir + generic_tickers[0] + '.csv'
    output_path_2 = out_dir + generic_tickers[1] + '.csv'   
    oi_1 = pd.read_csv(out_file_aux1,parse_dates=['date'],date_parser = dateparse,names=['contract_filename', 'date','open','high','low','close','contract_volume','contract_oi','total_volume','total_oi'])
    oi_2 = pd.read_csv(out_file_aux2,parse_dates=['date'],date_parser = dateparse,names=['contract_filename', 'date','open','high','low','close','contract_volume','contract_oi','total_volume','total_oi'])

    oi_1['specific_ticker'] = oi_1.apply(lambda row: get_specific_ticker_from_filename(row['contract_filename'], to_name), axis=1)
    oi_1['product'] = generic_tickers[0]
    for i in range(len(oi_1)-2):
        if oi_1.loc[i,'specific_ticker'] != oi_1.loc[i+1,'specific_ticker']:
            oi_1.loc[i,'is_last_trading_day'] = 1.0
        else:
            oi_1.loc[i,'is_last_trading_day'] = 0.0
    oi_1 = oi_1[['date','product','specific_ticker','open','high','low','close','is_last_trading_day','contract_volume','contract_oi','total_volume','total_oi']]

    oi_2['specific_ticker'] = oi_2.apply(lambda row: get_specific_ticker_from_filename(row['contract_filename'], to_name), axis=1)
    oi_2['product'] = generic_tickers[1]
    for i in range(len(oi_2)-2):
        if oi_2.loc[i,'specific_ticker'] != oi_2.loc[i+1,'specific_ticker']:
            oi_2.loc[i,'is_last_trading_day'] = 1.0
        else:
            oi_2.loc[i,'is_last_trading_day'] = 0.0
    oi_2 = oi_2[['date','product','specific_ticker','open','high','low','close','is_last_trading_day','contract_volume','contract_oi','total_volume','total_oi']]

    check(oi_1, oi_2)
    oi_1.to_csv(output_path_1, index=False)
    oi_2.to_csv(output_path_2, index=False)

def __main__():
    if len(sys.argv) > 1:
        to_name = sys.argv[2]
        product = sys.argv[1]
    else:
        print 'args: product to_name'
        sys.exit(0)
    process_futures(product, to_name)

if __name__ == '__main__':
    __main__();
