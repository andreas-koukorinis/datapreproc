# cython: profile=True
#!/usr/bin/env python
import os
import sys
import pandas as pd
from datetime import datetime,date,timedelta
from exchange_symbol_manager import ExchangeSymbolManager
    
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

def get_YYMM_from_exchange_code( code ):
    month_codes = {'F':'01','G':'02','H':'03','J':'04','K':'05','M':'06','N':'07','Q':'08','U':'09','V':'10','X':'11','Z':'12'}
    return code[1:]+month_codes[code[0]]

def get_start_date(product,label,exchange_symbol_manager,start_date):
    exch_code = get_exchange_specific(label)
    _basename = product.rsplit('_',1)[0]
    _req_code = _basename+exch_code
    while exchange_symbol_manager.get_exchange_symbol(start_date,product) != _req_code:
        start_date += timedelta(days=1)
    return start_date

def process_futures(num_contracts, product, to_name):    
    exchange_symbol_manager = ExchangeSymbolManager()
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

    output_path = '/home/cvdev/stratdev/data_cleaning/data/'
    dateparse = lambda x: datetime.strptime(x, '%Y%m%d').date() # Parse dates in format required by mysql i.e. YYYY-MM-DD
    generic_tickers = []
    generic_to_name = []
    for i in range(num_contracts):
        generic_tickers.append(product+'_'+str(i+1))
        generic_to_name.append(to_name+'_'+str(i+1))
    file_handlers = []
    output_dfs = []
    for ticker in generic_tickers:
        output_dfs.append(pd.DataFrame(columns=('date', 'product','specific_ticker', 'open', 'high', 'low', 'close', 'is_last_trading_day', 'contract_volume','contract_oi','total_volume','total_oi')))
    input_dfs = {}
    for filename in os.listdir(in_dir):
        fullname = in_dir + filename
        if '___' in filename: 
            YYMM = os.path.splitext(os.path.basename(fullname))[0].split('__')[1] 
        if '__' in filename: 
            YYMM = os.path.splitext(os.path.basename(fullname))[0].split('__')[1] 
        elif '_' in filename:
            YYMM = os.path.splitext(os.path.basename(fullname))[0].split('_')[1]
        input_dfs[YYMM] = pd.read_csv(fullname, parse_dates =['date'],date_parser=dateparse,names=['date','open','high','low','close','contract_volume','contract_oi','total_volume','total_oi'])
        input_dfs[YYMM] = input_dfs[YYMM].set_index('date')

    sorted_labels = sorted(input_dfs.keys(),key = convert_to_year_month)
    print sorted_labels
    start_date = datetime.strptime('1994-11-01', "%Y-%m-%d").date()
    end_date = datetime.strptime('2014-10-31', "%Y-%m-%d").date()
    delta = end_date-start_date
    for i in range(delta.days + 1):
        current_date = start_date + timedelta(days=i)

        for j in range( len(generic_tickers) ):
            YYMM = get_YYMM_from_exchange_code(exchange_symbol_manager.get_exchange_symbol(current_date,generic_to_name[j])[-3:]) 
            if YYMM not in input_dfs.keys():
                continue
            if current_date in input_dfs[YYMM].index:
                row = input_dfs[YYMM].loc[current_date]            
                output_dfs[j].loc[len(output_dfs[j])+1] = [ str(current_date), generic_to_name[j], to_name+get_exchange_specific(YYMM),row['open'], row['high'], row['low'], row['close'], '0.0', row['contract_volume'], row['contract_oi'], row['total_volume'], row['total_oi']]
 
    for j in range(len(output_dfs)):
        index_set = output_dfs[j].index.values
        count = 0 
        for i in range(len(index_set)-1):
            idx1 = index_set[i]
            idx2 = index_set[i+1]
            if output_dfs[j].loc[idx1, 'specific_ticker'] != output_dfs[j].loc[idx2, 'specific_ticker']:
                output_dfs[j].loc[idx1, 'is_last_trading_day'] = 1.0
                count += 1
        print '%dth contract : %d' % (j+1, count)
        print 'Min oi percent %f for index %f' % ((output_dfs[0]['contract_oi']/output_dfs[0]['total_oi']).min(), (output_dfs[0]['contract_oi']/output_dfs[0]['total_oi']).argmin())
    for i in range(len(generic_tickers)):
        output_dfs[i].to_csv(output_path + generic_to_name[i] + '.csv', index=False) # Save result to csv 

def __main__() :
    if len( sys.argv ) > 1:
        to_name = sys.argv[3]
        product = sys.argv[2]
        num_contracts = int(sys.argv[1])
    else:
        print 'python process_futures.py num_contracts product to_name'
        sys.exit(0)
    process_futures(num_contracts, product, to_name)

if __name__ == '__main__':
    __main__();
