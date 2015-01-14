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

def get_number_from_filename(filename):
    filename = os.path.splitext(filename)[0].split('/')[-1].rsplit('_',1)[1]
    return convert_to_year_month(filename[-4:])

def compare_ticker(ticker1, ticker2):
    month_codes_rev = {'F':'01','G':'02','H':'03','J':'04','K':'05','M':'06','N':'07','Q':'08','U':'09','V':'10','X':'11','Z':'12'}
    code1 = convert_to_year_month(ticker1[-2:] + month_codes_rev[ticker1[-3]])
    code2 = convert_to_year_month(ticker2[-2:] + month_codes_rev[ticker2[-3]])
    return code1 > code2

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

def fix_flips(in_dir, df):
    for i in range(len(df)-2):
        if compare_ticker(df.loc[i, 'specific_ticker'], df.loc[i+1, 'specific_ticker']):
            cmd = "cd %s;grep -H %s %s | sed 's/:/,/'"%(in_dir, df.loc[i+1,'date'], df.loc[i, 'contract_filename'])
            output = commands.getoutput(cmd).split(',')
            df.loc[i+1] = [output[0],output[1],output[2],output[3],output[4],output[5],output[6],output[7],output[8],output[9],df.loc[i, 'specific_ticker']]
    #df = df[['contract_filename', 'date','open','high','low','close','contract_volume','contract_oi','total_volume','total_oi']]
    return df

def get_second_contract(in_dir, df,  out_file_aux1, out_file_aux2, oi_1):
    cmd = "cat %s | awk -F, '{ print $1 }' | uniq"%(out_file_aux1)
    contract_filenames = commands.getoutput(cmd).split('\n')
    cmd = "cd %s; ls"%(in_dir)
    contract_filenames_ls = commands.getoutput(cmd).split('\n')
    contract_filenames_ls = sorted(contract_filenames_ls, key = get_number_from_filename) 
    j = 1
    f = open(out_file_aux2, 'w')
    mode = 0
    for i in range(len(df)-2):
        if mode == 0:
            if df.loc[i, 'contract_filename'] == contract_filenames[j]:
                j += 1
                if j == len(contract_filenames)-1:
                    mode = 1
        if mode == 0: 
            date =  df.loc[i, 'date']
            cmd = "cd %s;grep -H %s %s | sed 's/:/,/'"%(in_dir, str(date), contract_filenames[j])
            output = commands.getoutput(cmd)
            f.write(output)
        else:
            date =  df.loc[i, 'date']
            j = contract_filenames_ls.index(df.loc[i, 'contract_filename']) + 1
            cmd = "cd %s;grep -H %s %s | sed 's/:/,/'"%(in_dir, str(date), contract_filenames_ls[j])
            output = commands.getoutput(cmd)
            f.write(output)
    f.close()

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
    _aux_file1 = in_dir + product + '_0000.csv'
    _aux_file2 = in_dir + product + '__0000.csv'
    if os.path.exists(_aux_file1):
        os.remove(_aux_file1)
    if os.path.exists(_aux_file2):
        os.remove(_aux_file2)

    # Get first contract data based on highest open interest
    out_file_aux1 = out_dir + 'aux1_' + product + '.csv'
    cmd = "cd %s;for dd_ in `cat *csv | sort -g | awk -F, '{ print $1 }' | uniq`;do grep $dd_ *csv | sort -k7,7 -rg -t, | head -n1 | sed 's/:/,/' ;done"%(in_dir)
    output = commands.getoutput(cmd)
    output = ''.join(output.split('\r'))
    f = open(out_file_aux1, 'w')
    f.write(output)
    f.close()

    # Load the first contract data and check for flips 
    oi_1 = pd.read_csv(out_file_aux1, names=['contract_filename', 'date','open','high','low','close','contract_volume','contract_oi','total_volume','total_oi'])
    oi_1['specific_ticker'] = oi_1.apply(lambda row: get_specific_ticker_from_filename(row['contract_filename'], product), axis=1)
    if flips_present(oi_1):
        oi_1 = fix_flips(in_dir, oi_1)
        flips_present(oi_1)
    oi_1 = oi_1[['contract_filename', 'date','open','high','low','close','contract_volume','contract_oi','total_volume','total_oi']]
    out_file_new_aux1 = out_dir + 'aux1_new_' + product + '.csv'
    oi_1.to_csv(out_file_new_aux1, index=False, header=False)

    # Get the data for second contract based on first contract
    out_file_aux2 = out_dir + 'aux2_' + product + '.csv'
    get_second_contract(in_dir, oi_1, out_file_new_aux1, out_file_aux2, oi_1)

    # Process the data and output to db format
    generic_tickers = [to_name + '_1', to_name + '_2']
    output_path_1 = out_dir + generic_tickers[0] + '.csv'
    output_path_2 = out_dir + generic_tickers[1] + '.csv'   
    dateparse = lambda x: datetime.strptime(x, '%Y%m%d').date() # Parse dates in format required by mysql i.e. YYYY-MM-DD
    oi_1 = pd.read_csv(out_file_new_aux1,parse_dates=['date'],date_parser = dateparse,names=['contract_filename', 'date','open','high','low','close','contract_volume','contract_oi','total_volume','total_oi'])
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

    end_date = date(2014,10,31)
    oi_1 = oi_1[oi_1['date'] <= end_date]
    oi_2 = oi_2[oi_2['date'] <= end_date]
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
    __main__()
