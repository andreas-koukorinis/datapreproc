#!/usr/bin/env python
from datetime import datetime
import sys
import pandas as pd

parse = lambda x: datetime.strptime(x, '%Y%m%d')

def process_cftc(csi_symbol_file, path, csi_number, output_path):
    cftc_file_path = path+csi_number+'_63.txt'
    cftc_symbol_path = path+csi_symbol_file

    symbol_df = pd.DataFrame.from_csv(cftc_symbol_path, index_col='CommercialCsiNumber')
    symbol_df = symbol_df[symbol_df['CommercialDeliveryMonth']==63]
    product = symbol_df.loc[int(csi_number)]['Name'][4:-5]
    cftc_file_df = pd.DataFrame.from_csv(cftc_file_path, infer_datetime_format=True)
    cftc_file_df['Product'] = product
    cftc_file_df = cftc_file_df[['Product','LargeSpeculatorsLong','LargeSpeculatorsShort','CommercialTradersLong','CommercialTradersShort','SmallSpeculatorsLong','SmallSpeculatorsShort']]
    cftc_file_df.to_csv(output_path+product+'.csv')
    
def __main__() :
    if len( sys.argv ) == 5:
        csi_number = sys.argv[4] 
        csi_symbol_file = sys.argv[3]
        path = sys.argv[1]
        output_path = sys.argv[2]
    else:
        print 'python process_cftc.py path output_path csi_symbol_file csi_number'
        sys.exit(0)
    process_cftc(csi_symbol_file, path, csi_number, output_path)

if __name__ == '__main__':
    __main__();
