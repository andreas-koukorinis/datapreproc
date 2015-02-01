# cython: profile=True
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

def convert_to_yymm(number):
    year = (number/100)%100
    month = number%100
    yy = '0' + str(year) if year < 10 else str(year)
    mm = '0' + str(month) if month < 10 else str(month)
    return yy + mm

def get_number_from_filename(filename):
    if '___' in filename:
        filename = os.path.splitext(filename)[0].split('/')[-1].rsplit('___',1)[1]
    elif '__' in filename:
        filename = os.path.splitext(filename)[0].split('/')[-1].rsplit('__',1)[1]
    else:
        filename = os.path.splitext(filename)[0].split('/')[-1].rsplit('_',1)[1]
    return convert_to_year_month(filename[-4:])

def get_filename_from_number(number, initial):
    return initial + convert_to_yymm(number) + '.csv'

def shift_k_years(contract_filename, k):
    number = get_number_from_filename(contract_filename) + k*100
    return get_filename_from_number(number, os.path.splitext(contract_filename)[0].split('/')[-1][:-4])

def compare_ticker(ticker1, ticker2):
    month_codes_rev = {'F':'01','G':'02','H':'03','J':'04','K':'05','M':'06','N':'07','Q':'08','U':'09','V':'10','X':'11','Z':'12'}
    code1 = convert_to_year_month(ticker1[-2:] + month_codes_rev[ticker1[-3]])
    code2 = convert_to_year_month(ticker2[-2:] + month_codes_rev[ticker2[-3]])
    return code1 > code2

def flips_present(df):
    flips = []
    index_set = df.index.values
    #for i in range(len(df)-2):
    for i in range(len(index_set) - 1):
        idx1 = index_set[i]
        idx2 = index_set[i+1]
        if compare_ticker(df.loc[idx1, 'specific_ticker'], df.loc[idx2, 'specific_ticker']):
            flips.append(idx1)
    if len(flips) > 0:
        print 'ERROR: ',flips
        return True
    else:
        print 'No flips found'
        return False

def fix_flips(in_dir, df):
    index_set = df.index.values
    #for i in range(len(df)-2):
    for i in range(len(index_set) - 1):
        idx1 = index_set[i]
        idx2 = index_set[i+1]
        if compare_ticker(df.loc[idx1, 'specific_ticker'], df.loc[idx2, 'specific_ticker']):
            cmd = "cd %s;grep -H %s %s | sed 's/:/,/'"%(in_dir, df.loc[idx2,'date'], df.loc[idx1, 'contract_filename'])
            print cmd
            output = commands.getoutput(cmd).split(',')
            print output
            df.loc[idx2] = [output[0],output[1],output[2],output[3],output[4],output[5],output[6],output[7],output[8],output[9],df.loc[idx1, 'specific_ticker']]
    #df = df[['contract_filename', 'date','open','high','low','close','contract_volume','contract_oi','total_volume','total_oi']]
    return df

def get_contract_file_based_on_past_pattern(_first_contract_file, contract_filenames):
    lookback = -1
    while True:
        _past_first_contract_file = shift_k_years(_first_contract_file, lookback)
        if _past_first_contract_file in contract_filenames:
            j = contract_filenames.index(_past_first_contract_file)
            return shift_k_years(contract_filenames[j+1], -lookback)
        else:
            lookback -= 1

def get_second_contract(in_dir, df,  out_file_aux1, out_file_aux2, oi_1):

    cmd = "cat %s | awk -F, '{ print $1 }' | uniq"%(out_file_aux1)
    contract_filenames = commands.getoutput(cmd).split('\n')
    print contract_filenames
    cmd = "cd %s; ls"%(in_dir)
    contract_filenames_ls = commands.getoutput(cmd).split('\n')
    contract_filenames_ls = sorted(contract_filenames_ls, key = get_number_from_filename) 
    mode1_entered = False
    j = 1
    f = open(out_file_aux2, 'w')
    mode = 0
    index_set = df.index.values
    for i in range(len(index_set)):
        idx1 = index_set[i]
        if mode == 0:
            if df.loc[idx1, 'contract_filename'] == contract_filenames[j]:
                j += 1
                if j == len(contract_filenames):#-1:
                    mode = 1
        if mode == 0: 
            date =  df.loc[idx1, 'date']
            cmd = "cd %s;grep -H %s %s | sed 's/:/,/'"%(in_dir, str(date), contract_filenames[j])
            output = commands.getoutput(cmd)
            f.write(output)
        else:
            if not mode1_entered:
                mode1_entered = True
                print 'Mode 1 entered on %s'%df.loc[idx1, 'date']
            date =  df.loc[idx1, 'date']
            #j = contract_filenames_ls.index(df.loc[i, 'contract_filename']) + 1
            _first_contract_file = df.loc[idx1, 'contract_filename']
            _second_contract_file = get_contract_file_based_on_past_pattern(_first_contract_file, contract_filenames)
            if os.path.exists(in_dir+_second_contract_file):
                cmd = "cd %s;grep -H %s %s | sed 's/:/,/'"%(in_dir, str(date), _second_contract_file)
                output = commands.getoutput(cmd)
                f.write(output)
    f.close()

def process_futures(product, to_name):
    # Directory for the product
    dir1 = '/apps/data/csi/history_part1/' + product + '/'
    dir2 = '/apps/data/csi/history_part2/' + product + '/'
    dir3 = '/apps/data/csi/historical_futures1/' + product + '/'
    dir4 = '/apps/data/csi/historical_futures2/' + product + '/'
    if os.path.isdir(dir1):
        in_dir = dir1
    elif os.path.isdir(dir2):
        in_dir = dir2
    elif os.path.isdir(dir3):
        in_dir = dir3
    elif os.path.isdir(dir4):
        in_dir = dir4
    else:
        sys.exit('Not Found')       
    out_dir = '/home/cvdev/stratdev/data_cleaning/data/'

    # Remove 0000 file
    _aux_file1 = in_dir + product + '_0000.csv'
    _aux_file2 = in_dir + product + '__0000.csv'
    _aux_file3 = in_dir + product + '___0000.csv'
    if os.path.exists(_aux_file1):
        os.remove(_aux_file1)
    if os.path.exists(_aux_file2):
        os.remove(_aux_file2)
    if os.path.exists(_aux_file3):
        os.remove(_aux_file3)

    # Get first contract data based on highest open interest
    out_file_aux1 = out_dir + 'aux1_' + product + '.csv'
    #cmd = "cd %s;for dd_ in `cat *csv | sort -g | awk -F, '{ print $1 }' | uniq`;do grep $dd_ *csv | sort -k7,7 -rg -t, | head -n1 | sed 's/:/,/' ;done"%(in_dir)
    cmd = "bash first_contract.sh %s %s" % (in_dir, out_file_aux1)
    print cmd
    output = commands.getoutput(cmd)
    #output = ''.join(output.split('\r'))
    #f = open(out_file_aux1, 'w')
    #f.write(output)
    #f.close()

    # Load the first contract data and check for flips 
    oi_1 = pd.read_csv(out_file_aux1, names=['contract_filename', 'date','open','high','low','close','contract_volume','contract_oi','total_volume','total_oi'])
    oi_1 = oi_1[(oi_1['total_oi'] > 1000) & (oi_1['total_volume'] > 100) & (oi_1['contract_oi'] > 1000) & (oi_1['contract_volume'] > 100)] # Filter out dates with oi < 1000 and vol < 1000
    oi_1['specific_ticker'] = oi_1.apply(lambda row: get_specific_ticker_from_filename(row['contract_filename'], product), axis=1)
    oi_1.to_csv('data/check')
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
    print 'Min oi percent %f for index %f' % ((oi_1['contract_oi']/oi_1['total_oi']).min(), (oi_1['contract_oi']/oi_1['total_oi']).argmin())
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
