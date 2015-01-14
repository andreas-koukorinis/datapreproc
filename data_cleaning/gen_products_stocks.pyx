# cython: profile=True
#!/usr/bin/env python
import os
import sys
import pandas as pd
from datetime import datetime,date,timedelta

def gen_products( product_type, asset_class, table_name,currency, products ):
    if product_type == 'fund':
        df = pd.read_csv('fundfactsheet.csv',header=0)
    else:
        df = pd.read_csv('stockfactsheet.csv',header=0)
    df = df.set_index('Symbol')
    for product in products:
        row = df.loc[product]
        print "insert into products values ('%s','%s','%s','%s','%s','%s','%s','1','1','1','%s','%s');" % (product,row['CsiNumber'],product,row['Name'],table_name,product_type, asset_class,currency,row['Exchange'])

def __main__() :
    if len( sys.argv ) > 1:
        products = []
        product_type = sys.argv[1]
        asset_class = sys.argv[2]
        table_name = sys.argv[3]
        currency = sys.argv[4]
        for i in range(5,len(sys.argv)):
            product = sys.argv[i]
            products.append(product)
        gen_products( product_type, asset_class, table_name,currency, products )
    else:
        print 'python gen_products_stocks.py product_type asset_class table_name currency product1 product2 product3... productn'
        sys.exit(0)


if __name__ == '__main__':
    __main__();
