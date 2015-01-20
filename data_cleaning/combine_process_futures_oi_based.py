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
    if '__' in filename:
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
    print len(df)
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

def get_contract_file_based_on_past_pattern(_first_contract_file, contract_filenames):
    _past_first_contract_file = shift_k_years(_first_contract_file, -1)
    j = contract_filenames.index(_past_first_contract_file)
    return shift_k_years(contract_filenames[j+1], 1)

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
            if not mode1_entered:
                mode1_entered = True
                print 'Mode 1 entered on %s'%df.loc[i, 'date']
            date =  df.loc[i, 'date']
            #j = contract_filenames_ls.index(df.loc[i, 'contract_filename']) + 1
            _first_contract_file = df.loc[i, 'contract_filename']
            _second_contract_file = get_contract_file_based_on_past_pattern(_first_contract_file, contract_filenames)
            cmd = "cd %s;grep -H %s %s | sed 's/:/,/'"%(in_dir, str(date), _second_contract_file)
            output = commands.getoutput(cmd)
            f.write(output)
    f.close()

def process_futures(product1, product2, factor, to_name):
    in_dir = 'data/'
    file1_1 = in_dir + product1 + '/_1.csv'
    file1_2 = in_dir + product1 + '/_2.csv'
    file2_1 = in_dir + product2 + '/_1.csv'
    file2_2 = in_dir + product2 + '/_2.csv'
    output_path_1 = in_dir + to_name + '_1.csv'
    output_path_2 = in_dir + to_name + '_2.csv'
    dateparse = lambda x: datetime.strptime(x, '%Y-%m-%d').date()
    df1_1 = pd.read_csv(file1_1, header=0,parse_dates =['date'],date_parser=dateparse)
    df1_2 = pd.read_csv(file1_2, header=0,parse_dates =['date'],date_parser=dateparse)
    df2_1 = pd.read_csv(file2_1, header=0,parse_dates =['date'],date_parser=dateparse)
    df2_2 = pd.read_csv(file2_2, header=0,parse_dates =['date'],date_parser=dateparse)
    i = 0
    j = 0
    while True:
        if df1_1.loc[i, 'date'] < df2_1.loc[j, 'date']:
            i += 1
        elif df1_1.loc[i, 'date'] > df2_1.loc[j, 'date']:
            j += 1
        elif df1_1.loc[i, 'total_oi'] <= factor*df2_1.loc[j, 'total_oi']:
            break
        else:
            i += 1
            j += 1
    end_date1_1 = df1_1.loc[i, 'date']
    df1 = df1_1[df1_1['date'] < end_date1_1]
    df2 = df1_2[df1_2['date'] < end_date1_1]    
    df1 = pd.concat(df1, df2_1[df2_1['date'] >= end_date1_1])
    df2 = pd.concat(df2, df2_2[df2_2['date'] >= end_date1_1])
    df1.to_csv(output_path_1, index=False)
    df2.to_csv(output_path_2, index=False)

def __main__():
    if len(sys.argv) > 1:
        to_name = sys.argv[4]
        factor = float(sys.argv[3])
        product2 = sys.argv[2]
        product1 = sys.argv[1]
    else:
        print 'args: product1 product2 factor to_name'
        sys.exit(0)
    combine_process_futures(product1, product2, factor, to_name)
    

if __name__ == '__main__':
    __main__()
