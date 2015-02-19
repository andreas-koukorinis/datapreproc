#!/usr/bin/env python
import os
import sys
import pandas as pd
from datetime import datetime,date,timedelta

def gen_products( asset_class, table_name, products ):
    df = pd.read_csv('commodityfactsheet.csv',header=0)
    df = df.set_index('ExchangeSymbol')
    for product in products:
        row = df.loc[product]
        if len(row) >= 2 and len(row) < 20:
            df1 = df[df['SymbolCommercial'] == product]
            row = df1.loc[product]
        conversion_factor = float(row['FullPointValue'])
        min_tick_value = float(row['TickValue'])
        min_tick = min_tick_value/conversion_factor
        print "insert into products values ('%s','%s','%s','%s','%s','future','%s','%f','%f','%f','%s','%s');" % (product+'_1',row['CommercialCsiNumber'],row['SymbolCommercial'],row['Name']+' Future 1',table_name,asset_class,min_tick,min_tick_value,conversion_factor,row['Currency'],row['Exchange'])
        print "insert into products values ('%s','%s','%s','%s','%s','future','%s','%f','%f','%f','%s','%s');" % (product+'_2',row['CommercialCsiNumber'],row['SymbolCommercial'],row['Name']+' Future 2',table_name,asset_class,min_tick,min_tick_value,conversion_factor,row['Currency'],row['Exchange'])


def __main__() :
    if len( sys.argv ) > 1:
        products = []
        asset_class = sys.argv[1]
        table_name = sys.argv[2]
        for i in range(3,len(sys.argv)):
            product = sys.argv[i]
            products.append(product)
        gen_products( asset_class, table_name, products )
    else:
        print 'python gen_products.py asset_class table_name product1 product2 product3... productn'
        sys.exit(0)


if __name__ == '__main__':
    __main__();
