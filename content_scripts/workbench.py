#!/usr/bin/env python 
"""API for building workbench"""

import hashlib
import json
from random import randint
import sys
import MySQLdb
import pandas as pd
import numpy as np

def db_connect():
    global db, db_cursor
    try:
        with open('/spare/local/credentials/readonly_credentials.txt') as f:
            credentials = [line.strip().split(':') for line in f.readlines()]
    except IOError:
        sys.exit('No credentials file found')
    try:
        for user_id,password in credentials:
            db = MySQLdb.connect(host='fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com', user=user_id, passwd=password, db='webapp')
            db_cursor = db.cursor(MySQLdb.cursors.DictCursor) 
    except MySQLdb.Error:
        sys.exit("Error in DB connection")

def db_close():
    cursor = db.cursor()
    cursor.close()
    del cursor
    db.close() 

def get_base_strategies():
    """Get base stratefy ids and descriptions
    Returns:
        Base strategy ids and their correspoding descriptions(dict)
        e.g. {'12':'MVO','134':'TRVP','123':'SMS'}
    """
    db_connect()
    query = "SELECT * FROM base_strategies"
    base_strats_df = pd.read_sql(query, con=db)
    db_close()

    base_strats = {}
    for i in xrange(len(base_strats_df.index)):
        base_strats[base_strats_df.iloc[i]['id']] = base_strats_df.iloc[i]['base_strategy']
    
    return base_strats

def get_params_for_base_strategy(base_strategy_id):
    """Get available parameter combinations for that strategy
    Parameters:
        1. base_strategy_id(string)
    Returns:
        All permissible sets of parameter configurations for a selected base strategy(List of tuples of ids and parameter combination dicts)
        e.g. [('2145',{'average_discretized_trend':21, 'average_std_lookback':21}),
              ('2123',{'average_discretized_trend':252, 'average_std_lookback':252}),
              ('1123',{'average_discretized_trend':[21, 63, 252], 'average_std_lookback':63})]
    """
    db_connect()
    query = "SELECT simulation_id, param_combn FROM workbench_strategies WHERE base_strategy_id='%s'"%base_strategy_id
    strats_df = pd.read_sql(query, con=db)
    db_close()
    
    params_for_strategy = []
    for i in xrange(len(strats_df.index)):
        id_params_dict = json.loads(strats_df.iloc[i]['param_combn'])
        id_params_dict['id'] = strats_df.iloc[i]['simulation_id']
        params_for_strategy.append(id_params_dict)
    
    return params_for_strategy

def get_stats_for_strategy(simulation_id):
    """Maps combination of parameters and base strategy to a strategy id
       and returns stats for that strategy
    Parameters:
        1. simulation_id(string)
    Returns:
        All stats (dict)
        e.g. {'daily_log_returns':[0.1,0.2],'dates':['2015-05-05','2015-05-06'],'sharpe':1.5}
    """
    db_connect()
    query = "SELECT * FROM wb_strategies where id = %s"%simulation_id
    strategy_df = pd.read_sql(query, con=db)
    db_close()
    print strategy_df
    daily_weights = json.loads(strategy_df.iloc[0]['daily_weights'])
    dates = json.loads(strategy_df.iloc[0]['dates'])
    log_returns = json.loads(strategy_df.iloc[0]['daily_log_returns'])
    leverage =  json.loads(strategy_df.iloc[0]['daily_leverage'])
    strategy_df['daily_log_returns'] = [log_returns]
    strategy_df['daily_leverage'] = [leverage]
    strategy_df['products'] = [daily_weights[0]]
    
    # Unpack weights from daily_weights 
    for i, product in enumerate(strategy_df['products'][0]):
        allocation = []
        for j in  xrange(1,len(dates)+1):
           allocation.append(daily_weights[j][i])
        strategy_df[product] = [allocation]
    
    columns = strategy_df['products'][0] + ['daily_log_returns', 'daily_leverage']
    
    # Round to reduce precision and data being sent
    for c in columns:
        strategy_df[c] = [[round(x,3) for x in strategy_df.iloc[0][c]]]

    # Drop unserializable columns
    strategy_df = strategy_df.drop('updated_at', 1)
    strategy_df = strategy_df.drop('created_at', 1)
    strategy_df = strategy_df.drop('daily_weights', 1)
    
    return strategy_df.iloc[0].to_dict()
