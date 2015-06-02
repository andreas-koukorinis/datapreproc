#!/usr/bin/env python 
"""API for building workbench"""

import hashlib
import json
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
    base_strategies = {'1':'Fixed Allocation Strategy(Futures)', '2':'Risk Aware Allocation Strategy(Futures)',\
                       '3':'Correlation and Risk Aware Allocation Strategy(Futures)', '4':'Equalized Risk Allocation Strategy(Futures)',\
                       '5':'Equalized Risk Momentum Strategy(Futures)', '6':'Variance Aware Momentum Strategy(Futures)',\
                       '7':'Discretized Momentum Strategy(Futures)', '8':'VIX Futures based Carry Strategy',\
                       '9':'Fixed Allocation Strategy(ETFs)', '10':'Risk Aware Allocation Strategy(ETFs)',\
                       '11':'Correlation and Risk Aware Allocation Strategy(ETFs)', '12':'Equalized Risk Allocation Strategy(ETFs)',\
                       '13':'Equalized Risk Momentum Strategy(ETFs)', '14':'Variance Aware Momentum Strategy(ETFs)',\
                       '15':'Discretized Momentum Strategy(ETFs)'}
    return json.dumps(base_strategies)

def get_params_for_base_strategy(base_strategy_id):
    """Get available parameter combinations for that strategy
    Parameters:
        1. base_strategy_id(string)
    Returns:
        All permissible sets of parameter configurations for a selected base strategy(List of dicts)
        e.g. [{'average_discretized_trend':21, 'average_std_lookback':21},
              {'average_discretized_trend':252, 'average_std_lookback':252},
              {'average_discretized_trend':[21, 63, 252], 'std_lookback':63}]
    """
    cwas_futures_params = ['sector_allocation', 'rebalancing_frequency']
    cwas_futures_combns = [([1, 1, 1, 1, 1], 21), \
                           ([1, 1, 1, 1, 1], 63), \
                           ([1, 1, 1, 1, 1], 252), \
                           ([0, 0, 0, 0, 1], 21), \
                           ([0, 0, 0, 1, 0], 21), \
                           ([0, 0, 1, 0, 0], 21), \
                           ([0, 1, 0, 0, 0], 21), \
                           ([1, 0, 0, 0, 0], 21), \
                          ]
    
    cwas_etfs_params = ['sector_allocation', 'rebalancing_frequency']
    cwas_etfs_combns = [([1, 1, 1], 21), \
                        ([1, 1, 1], 63), \
                        ([1, 1, 1], 252), \
                        ([0, 0, 1], 21), \
                        ([0, 1, 0], 21), \
                        ([1, 0, 0], 21), \
                       ]

    trvp_futures_params = ['average_std_lookback', 'rebalancing_frequency', 'target_risk']
    trvp_futures_combns = [([21], 21, 10), \
                           ([252], 21, 10), \
                           ([21, 63, 252], 21, 10), \
                           ([21], 126,10), \
                           ([21, 63, 252], 126, 10), \
                           ([21], 126, 10), \
                           ([21], 21, 20), \
                           ([21], 126, 20), \
                          ]
    
    trvp_etfs_params = ['average_std_lookback', 'rebalancing_frequency', 'target_risk']
    trvp_etfs_combns = [([21], 21, 10), \
                        ([252], 21, 10), \
                        ([21, 63, 252], 21, 10), \
                        ([21], 126, 10), \
                        ([21, 63, 252], 126, 10), \
                        ([21], 126,10), \
                        ([21], 21, 20), \
                        ([21], 126, 20), \
                       ]

    trmshc_futures_params = ['average_std_lookback', 'rebalancing_frequency', 'target_risk']
    trmshc_futures_combns = [([21], 21, 10), \
                             ([252], 21, 10), \
                             ([21, 63, 252], 21, 10), \
                             ([21], 126,10), \
                             ([21, 63, 252], 126, 10), \
                             ([21], 126, 10), \
                             ([21], 21, 20), \
                             ([21], 126, 20), \
                            ]
    
    trmshc_etfs_params = ['average_std_lookback', 'rebalancing_frequency', 'target_risk']
    trmshc_etfs_combns = [([21], 21, 10), \
                          ([252], 21, 10), \
                          ([21, 63, 252], 21, 10), \
                          ([21], 126, 10), \
                          ([21, 63, 252], 126, 10), \
                          ([21], 126,10), \
                          ([21], 21, 20), \
                          ([21], 126, 20), \
                         ]

    trerc_futures_params = ['average_std_lookback', 'rebalancing_frequency', 'target_risk']
    trerc_futures_combns = [([21], 21, 10), \
                            ([252], 21, 10), \
                            ([21, 63, 252], 21, 10), \
                            ([21], 126,10), \
                            ([21, 63, 252], 126, 10), \
                            ([21], 126, 10), \
                            ([21], 21, 20), \
                            ([21], 126, 20), \
                           ]
    
    trerc_etfs_params = ['average_std_lookback', 'rebalancing_frequency', 'target_risk']
    trerc_etfs_combns = [([21], 21, 10), \
                         ([252], 21, 10), \
                         ([21, 63, 252], 21, 10), \
                         ([21], 126, 10), \
                         ([21, 63, 252], 126, 10), \
                         ([21], 126,10), \
                         ([21], 21, 20), \
                         ([21], 126, 20), \
                        ]

    treerc_futures_params = ['average_std_lookback', 'score', 'score_lookback', 'rebalancing_frequency', 'target_risk']
    treerc_futures_combns = [([21], 'ExpectedReturns', [252], 21 , 10), \
                             ([252], 'ExpectedReturns', [252], 21 , 10), \
                             ([21, 63, 252], 'ExpectedReturns', [252], 21 , 10), \
                             ([21], 'ExpectedReturns', [252], 252 , 10), \
                             ([21], 'ExpectedReturns', [252], 21 , 20), \
                             ([21, 63, 252], 'ExpectedReturns', [252], 21 , 20),
                             ([21], 'AverageDiscretizedTrend', [21,63,252], 21 , 10), \
                             ([21], 'AverageDiscretizedTrend', [21,63,252], 252 , 10), \
                             ([252], 'AverageDiscretizedTrend', [21,63,252], 21 , 10), \
                             ([21, 63, 252], 'AverageDiscretizedTrend', [21,63,252], 21 , 10), \
                             ([21], 'AverageDiscretizedTrend', [21,63,252], 21 , 20), \
                             ([21, 63, 252], 'AverageDiscretizedTrend', [21,63,252], 21 , 20), \
                            ]
    
    treerc_etfs_params = ['average_std_lookback', 'score', 'score_lookback', 'rebalancing_frequency', 'target_risk']
    treerc_etfs_combns = [([21], 'ExpectedReturns', [252], 21 , 10), \
                          ([252], 'ExpectedReturns', [252], 21 , 10), \
                          ([21, 63, 252], 'ExpectedReturns', [252], 21 , 10), \
                          ([21], 'ExpectedReturns', [252], 252 , 10), \
                          ([21], 'ExpectedReturns', [252], 21 , 20), \
                          ([21, 63, 252], 'ExpectedReturns', [252], 21 , 20),
                          ([21], 'AverageDiscretizedTrend', [21,63,252], 21 , 10), \
                          ([21], 'AverageDiscretizedTrend', [21,63,252], 252 , 10), \
                          ([252], 'AverageDiscretizedTrend', [21,63,252], 21 , 10), \
                          ([21, 63, 252], 'AverageDiscretizedTrend', [21,63,252], 21 , 10), \
                          ([21], 'AverageDiscretizedTrend', [21,63,252], 21 , 20), \
                          ([21, 63, 252], 'AverageDiscretizedTrend', [21,63,252], 21 , 20), \
                         ]

    trvc_params = ['average_std_lookback']
    trvc_combns = [([21],), \
                   ([63],), \
                   ([252],), \
                   ([21, 63, 252],), \
                  ]

    mvo_futures_params = ['average_std_lookback', 'expected_returns_lookback', 'rebalancing_frequency', 'target_risk']
    mvo_futures_combns = [([21], [252], 21, 10), \
                          ([21, 63, 252], [252], 21, 10),\
                          ([21], [21], 21, 10), \
                          ([21], [63], 21, 10), \
                          ([21, 63, 252], [252], 63, 10), \
                          ([21, 63, 252], [252], 252, 10), \
                          ([21, 63, 252], [252], 21, 20), \
                          ([21], [252], 21, 20), \
                         ]

    mvo_etfs_params = ['average_std_lookback', 'expected_returns_lookback', 'rebalancing_frequency', 'target_risk']
    mvo_etfs_combns = [([21], [252], 21, 10), \
                          ([21, 63, 252], [252], 21, 10),\
                          ([21], [21], 21, 10), \
                          ([21], [63], 21, 10), \
                          ([21, 63, 252], [252], 63, 10), \
                          ([21, 63, 252], [252], 252, 10), \
                          ([21, 63, 252], [252], 21, 20), \
                          ([21], [252], 21, 20), \
                      ]
    
    sms_futures_params = ['average_std_lookback', 'trend_lookback', 'rebalancing_frequency', 'target_risk']
    sms_futures_combns = [([21], [21], 21, 10), \
                          ([21], [21, 63, 252], 21, 10), \
                          ([21], [63], 21, 10), \
                          ([21], [252], 21, 10), \
                          ([21, 63, 252], [21, 63, 252], 21 , 10), \
                          ([21, 63, 252], [21, 63, 252], 21 , 20), \
                          ([21], [21, 63, 252], 21 , 20), \
                          ([21, 63, 252], [21, 63, 252], 63, 10), \
                          ([21, 63, 252], [21, 63, 252], 252, 10), \
                         ]
    
    sms_etfs_params = ['average_std_lookback', 'trend_lookback', 'rebalancing_frequency', 'target_risk']
    sms_etfs_combns = [([21], [21], 21, 10), \
                          ([21], [21, 63, 252], 21, 10), \
                          ([21], [63], 21, 10), \
                          ([21], [252], 21, 10), \
                          ([21, 63, 252], [21, 63, 252], 21 , 10), \
                          ([21, 63, 252], [21, 63, 252], 21 , 20), \
                          ([21], [21, 63, 252], 21 , 20), \
                          ([21, 63, 252], [21, 63, 252], 63, 10), \
                          ([21, 63, 252], [21, 63, 252], 252, 10), \
                      ]

    strat_to_combn_map = {'1':(cwas_futures_params, cwas_futures_combns), '2':(trvp_futures_params, trvp_futures_combns),\
                          '3':(trmshc_futures_params, trmshc_futures_combns), '4':(trerc_futures_params, trerc_futures_combns),\
                          '5':(treerc_futures_params, treerc_futures_combns), '6':(mvo_futures_params, mvo_futures_combns),\
                          '7':(sms_futures_params, sms_futures_combns), '8':(trvc_params, trvc_combns),\
                          '9':(cwas_etfs_params, cwas_etfs_combns), '10':(trvp_etfs_params, trvp_etfs_combns),\
                          '11':(trmshc_etfs_params, trmshc_etfs_combns), '12':(trerc_etfs_params, trerc_etfs_combns),\
                          '13':(treerc_etfs_params, treerc_etfs_combns), '14':(mvo_futures_params, mvo_futures_combns),\
                          '15':(sms_etfs_params, sms_etfs_combns)}

    params_for_strategy = []
    for i in xrange(len(strat_to_combn_map[base_strategy_id][1])):
        params_dict = {}
        for j in xrange(len(strat_to_combn_map[base_strategy_id][0])):
            params_dict[strat_to_combn_map[base_strategy_id][0][j]] = strat_to_combn_map[base_strategy_id][1][i][j]
        params_for_strategy.append(params_dict) 
    
    return json.dumps(params_for_strategy)

def get_stats_for_strategy(base_strategy_id, chosen_params={}):
    """Maps combination of parameters and base strategy to a strategy id
       and returns stats for that strategy
    Parameters:
        1. base_strategy_id(string)
        2. chosen_params(dict)
        Keys should correspond to given base_strategy
    Returns:
        All stats (dict)
        e.g. {'daily_log_returns':[0.1,0.2],'dates':['2015-05-05','2015-05-06'],'sharpe':1.5}
    """
    if base_strategy_id not in json.loads(get_base_strategies()).keys():
        return '{}'
    
    # Send mock data from db for now
    db_connect()        
    query = "SELECT id FROM strategies"
    ids_df = pd.read_sql(query, con=db)

    try:
        base_strategy_id = int(base_strategy_id)
    except:
        base_strategy_id = 10

    strategy_id = base_strategy_id*20 % len(ids_df.index)

    query = "SELECT * FROM strategies where id = %s"%ids_df.iloc[strategy_id]['id']
    strategy_df = pd.read_sql(query, con=db)
    strategy_df = strategy_df.drop('updated_at', 1)
    strategy_df = strategy_df.drop('created_at', 1)
    db_close()

    return json.dumps(strategy_df.iloc[0].to_dict())
