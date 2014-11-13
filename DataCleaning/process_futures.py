#!/usr/bin/env python
import os
import sys
import pandas as pd
from datetime import datetime,date,timedelta
    
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
    
def process_futures(num_contracts,product,to_name,folder,th1,th2):
    path = '/apps/data/csi/historical_futures'+folder+'/'
    output_path = '/home/cvdev/stratdev/DataCleaning/Data/'
    dateparse = lambda x: datetime.strptime(x, '%Y%m%d').date() # Parse dates in format required by mysql i.e. YYYY-MM-DD
    directory = path + product + '/'
    generic_tickers = []
    generic_to_name = []
    for i in range(num_contracts):
        generic_tickers.append(product+'_'+str(i+1))
        generic_to_name.append(to_name+'_'+str(i+1))
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
    #print sorted_labels
    settlement_dates = []
    for label in sorted_labels:
        #print label,len(input_dfs[label])-2
        df = input_dfs[label]
        updated = 0
        for i in range(len(df)-2,-1,-1):
            fvol1 = float(df.loc[i]['contract_volume'])
            fvol2 = float(df.loc[i+1]['contract_volume'])
            tvol1 = float(df.loc[i]['total_volume'])
            tvol2 = float(df.loc[i+1]['total_volume'])
            if fvol1 == 0:
                v1 = 0
            else:
                v1 = fvol1/tvol1
            if fvol2 ==0:
                v2 = 0
            else:
                v2 = fvol2/tvol2
        #    #print 'h',df.loc[i]['date'],label,v1,v2,fvol1,fvol2,tvol1,tvol2
            if v1 - v2 > th1 and tvol1 != 0 and tvol2 !=0 and (len(settlement_dates)==0 or df.loc[i]['date'] > settlement_dates[-1]):
                _last_trading_date = df.loc[i]['date']
                print 'Updated',_last_trading_date,len(df)-i,label
                updated=1
                break
        if updated==0:
            _last_trading_date = _last_trading_date + timedelta(days=90)
        #_last_trading_date = df.loc[len(df)-7]['date']
        print label,_last_trading_date,i
        settlement_dates.append(_last_trading_date)
        input_dfs[label] = input_dfs[label].set_index('date')  
    start_date = input_dfs[sorted_labels[0]].index.values[0]
    end_date = settlement_dates[-num_contracts] 
    delta = end_date-start_date
    current_idx = 0
    for i in range(delta.days + 1):
        current_date = start_date + timedelta(days=i)  
        if current_date > settlement_dates[current_idx]:
            current_idx = current_idx+1
        if settlement_dates[current_idx]==current_date:
            _is_last_trading_day = 1
        else:
            _is_last_trading_day = 0
        for j in range( len(generic_tickers) ):
            YYMM = sorted_labels[current_idx + j] 
            if current_date in input_dfs[YYMM].index:
                row = input_dfs[YYMM].loc[current_date]            
                output_dfs[j].loc[len(output_dfs[j])+1] = [ str(current_date), generic_to_name[j], to_name+get_exchange_specific(YYMM),row['open'], row['high'], row['low'], row['close'], _is_last_trading_day, row['contract_volume'], row['contract_oi'], row['total_volume'], row['total_oi']]

    for i in range(len(generic_tickers)):
        output_dfs[i].to_csv(output_path+generic_to_name[i]+'.csv',index=False) # Save result to csv 

def __main__() :
    if len( sys.argv ) > 1:
        to_name = sys.argv[4]
        product = sys.argv[3]
        num_contracts = int(sys.argv[1])
        folder = sys.argv[2]
        threshold1 = float(sys.argv[5])
        threshold2 = float(sys.argv[6])
    else:
        print 'python process_futures_1.py num_contracts folder product to_name threshold1 threshold2'
        sys.exit(0)
    process_futures( num_contracts, product, to_name, folder, threshold1, threshold2 )


if __name__ == '__main__':
    __main__();
