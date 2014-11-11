#!/usr/bin/env python
import os
import sys
import pandas as pd
from datetime import datetime,date,timedelta

def get_expiry_YYMM(ticker_number, current_date):
    year = int(current_date.year)
    month = int(current_date.month)
    req_month = 3*int((month+2)/3) + (ticker_number-1)*3
    year = year + max(0,int((req_month)/12))
    month = (req_month-1)%12 + 1
    s = str(year)[2] + str(year)[3]
    if month <10:
        s = s + '0' + str(month)
    else:
        s = s + str(month)
    return s
    
def convert_to_year_month(YYMM):
    yy = int(YYMM[0:2])
    mm = int(YYMM[2:4])
    if yy >50:
        year = 1900 + yy
    else:
        year = 2000 + yy
    return year*100 + mm

def get_exchange_specific(YYMM):
    YY,MM = YYMM[0:2],YYMM[2:4]
    month_codes = {'01':'F','02':'G','03':'H','04':'J','05':'K','06':'M','07':'N','08':'Q','09':'U','10':'V','11':'X','12':'Z'}
    return month_codes[MM]+YY
    
def process_futures(num_contracts,product,to_name):
    path = '/home/cvdev/data/csi/historical_futures1/'
    output_path = '/home/cvdev/stratdev/DataCleaning/Data/'
    dateparse = lambda x: datetime.strptime(x, '%Y%m%d').date() # Parse dates in format required by mysql i.e. YYYY-MM-DD
    directory = path + product + '/'
    generic_tickers = []
    generic_to_name = []
    for i in range(num_contracts):
        generic_tickers.append(product+str(i+1))
        generic_to_name.append(to_name+str(i+1))
    file_handlers = []
    output_dfs = []
    for ticker in generic_tickers:
        output_dfs.append(pd.DataFrame(columns=('date', 'product','specific_ticker', 'open', 'high', 'low', 'close', '_is_last_trading_day', 'contract_volume','contract_oi','total_volume','total_oi')))
    input_dfs = {}
    for filename in os.listdir(directory):
        fullname = directory+filename
        if '__' in filename: 
            YYMM = os.path.splitext(os.path.basename(fullname))[0].split('__')[1] 
        elif '_' in filename:
            YYMM = os.path.splitext(os.path.basename(fullname))[0].split('_')[1]
        input_dfs[YYMM] = pd.read_csv(fullname,parse_dates =['date'],date_parser=dateparse,names=['date','open','high','low','close','contract_volume','contract_oi','total_volume','total_oi'])
    sorted_labels = sorted(input_dfs.keys(),key = convert_to_year_month)
    settlement_dates = []
    for label in sorted_labels:
        settlement_dates.append(input_dfs[label].loc[len(input_dfs[label])-2]['date'])
        input_dfs[label] = input_dfs[label].set_index('date')  
    start_date = input_dfs[sorted_labels[0]].index.values[0]
    end_date = settlement_dates[-num_contracts] 
    delta = end_date-start_date
    current_idx = 0
    for i in range(delta.days + 1):
        current_date = start_date + timedelta(days=i)  
        if current_date > settlement_dates[current_idx]:
            current_idx = current_idx+1
        inall = True
        insome = False
        for j in range( len(generic_tickers) ):
            YYMM = sorted_labels[current_idx + j]        
            if current_date in input_dfs[YYMM].index:
                insome = True
            else:
                inall = False
        if inall:
            if settlement_dates[current_idx]==current_date:
                _is_last_trading_day = 1
            else:
                _is_last_trading_day = 0
            for j in range( len(generic_tickers) ):
                YYMM = sorted_labels[current_idx + j] 
                row = input_dfs[YYMM].loc[current_date]
                get_exchange_specific(YYMM)                
                output_dfs[j].loc[len(output_dfs[j])+1] = [ str(current_date), generic_to_name[j], generic_to_name[j].rstrip('0123456789')+get_exchange_specific(YYMM),row['open'], row['high'], row['low'], row['close'], _is_last_trading_day, row['contract_volume'], row['contract_oi'], row['total_volume'], row['total_oi']]
        elif not inall and insome:
            print 'skipping %s'%(str(current_date))
    for i in range(len(generic_tickers)):
        output_dfs[i].to_csv(output_path+generic_to_name[i]+'.csv',index=False) # Save result to csv 

def __main__() :
    if len( sys.argv ) > 1:
        to_name = sys.argv[3]
        product = sys.argv[2]
        num_contracts = int(sys.argv[1])
    else:
        print 'python process_db.py num_contracts product to_name'
        sys.exit(0)
    process_futures( num_contracts, product, to_name )

#print get_expiry_YYMM(7,datetime.strptime('20140301', "%Y%m%d").date())
if __name__ == '__main__':
    __main__();
