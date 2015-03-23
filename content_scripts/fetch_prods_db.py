#!/usr/bin/env python

import sys
import argparse
import MySQLdb
import pandas as pd
import numpy as np

#Connect to the database and return the db cursor if the connection is successful
def db_connect():
    try:
        db = MySQLdb.connect(host="fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvqrdonly",passwd="QPlumReadInc", db="daily_qplum")
        return (db,db.cursor(MySQLdb.cursors.DictCursor))
    except MySQLdb.Error:
        sys.exit("Error In DB Connection")

def db_close(db):
    cursor = db.cursor()
    cursor.close()
    del cursor
    db.close()  

def fetch_from_db(db_conn, db_cursor, prod, output_path):
    query = "SELECT * FROM products WHERE product = '%s';" % prod 
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    
    if len(rows) > 0:
        table = rows[0]['table']
        prod_type = rows[0]['type']
        query = "SELECT * from %s WHERE product = '%s';" % (table, prod)
        df = pd.read_sql(query, con=db_conn)
        df = df.convert_objects(convert_numeric=True)
        if prod_type == 'index':
            df['Log Returns'] = np.log(df['close'].shift(1)/ df['close'])
        elif prod_type == 'future':
            df['Log Returns'] = np.log(df['close'].shift(1)/ df['close'])
            curr_contract = int(prod[-1])
            # Use next contract's close price on the day after last trading day
            query = "SELECT * from %s WHERE product = '%s';" % (table, prod[:-1]+str(curr_contract+1))
            df2 = pd.read_sql(query, con=db_conn)
            df2 = df.convert_objects(convert_numeric=True)
            for idx in df[df.is_last_trading_day==1.0].index:
                df.ix[idx+1, 'Log Returns'] = np.log(df.iloc[idx+1]['close']/df2.iloc[idx]['close'])
        else:
            # Use split adjusted and backward dividend adjusted values to calcualte log returns 
            # for ETFs, funds and stocks
            df['Log Returns'] = np.log(df['backward_adjusted_close'].shift(1)/ df['backward_adjusted_close'])
        # Set default value for first date
        df.loc[0,'Log Returns'] = 0
        df.to_csv(output_path+prod+'.csv', index=False)
    else:
        print "Product %s not in DB. If product is future specify contract like VX_1 or ES_2" % prod

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', nargs='+', help='List of products\nEg: -p VIX ES_1 VX_1', default='VIX', dest='products')
    parser.add_argument('-o', type=str, help='Output Path\nEg: -o /home/user/data/', default='./', dest='output_path')
    args = parser.parse_args()

    db_conn, db_cur = db_connect()

    for prod in args.products:
        fetch_from_db(db_conn, db_cur, prod, args.output_path)
    
    db_close(db_conn)

if __name__ == '__main__':
    main()