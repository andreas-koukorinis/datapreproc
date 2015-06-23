#!/usr/bin/env python 
"""API for building workbench"""

import hashlib
import json
from random import randint
import sys
import yaml
import MySQLdb
import pandas as pd
import numpy
import datetime

sector_map = {
    "fFDAX" : "equity",
    "fLFZ" : "equity",
    "fMFX" : "equity",
    "fNKD" : "equity",
    "fFTI" : "equity",
    "fFESX" : "equity",
    "fSXF" : "equity",
    "fHSI" : "equity",
    "fFSMI" : "equity",
    "fES" : "equity",
    "fEMD" : "equity",
    "fJNK" : "equity",
    "fTOPIX" : "equity",
    "fALSI" : "equity",
    "f6A" : "currency",
    "f6B" : "currency",
    "f6C" : "currency",
    "f6M" : "currency",
    "f6N" : "currency",
    "f6J" : "currency",
    "f6S" : "currency",
    "fLFR" : "fixed_income",
    "fCGB" : "fixed_income",
    "fZT" : "fixed_income",
    "fZF" : "fixed_income",
    "fZN" : "fixed_income",
    "fZB" : "fixed_income",
    "fFGBL" : "fixed_income",
    "fFGBM" : "fixed_income",
    "fJGBL" : "fixed_income",
    "fGC" : "metals",
    "fSI" : "metals",
    "fHG" : "metals",
    "fPL" : "metals",
    "fPA" : "metals",
    "fKC" : "agriculture",
    "fCT" : "agriculture",
    "fSB" : "agriculture",
    "fCC" : "agriculture",
    "fLH" : "agriculture",
    "fZC" : "agriculture",
    "fZW" : "agriculture",
    "fZS" : "agriculture",
    "fZM" : "agriculture",
    "fZL" : "agriculture",
    "IEMG" : "equity",
    "LQD" : "fixed_income",
    "MUB" : "fixed_income",
    "SHV" : "fixed_income",
    "TIP" : "fixed_income",
    "VBR" : "equity",
    "VEA" : "equity",
    "VIG" : "equity",
    "VTSMX" : "equity",
    "VNQ" : "real_estate",
    "VOE" : "equity",
    "VT" : "equity",
    "VTI" : "equity",
    "VTIP" : "fixed_income",
    "VTV" : "equity",
    "VWO" : "equity",
    "VWOB" : "fixed_income",
    "VXUS" : "equity",
    "BND" : "fixed_income",
    "BNDX" : "fixed_income",
    "VBLTX" : "fixed_income"
}

def db_connect():
    global db, db_cursor
    try:
        with open('/spare/local/credentials/readonly_credentials.txt') as f:
            credentials = [line.strip().split(':') for line in f.readlines()]
    except IOError:
        sys.exit('No credentials file found')
    try:
        for user_id,password in credentials:
            db = MySQLdb.connect(host='fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com', user=user_id, passwd=password, db='workbench')
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
        base_strats[base_strats_df.iloc[i]['id']] = {}
        base_strats[base_strats_df.iloc[i]['id']]['name'] = base_strats_df.iloc[i]['base_strategy']
        base_strats[base_strats_df.iloc[i]['id']]['description'] = base_strats_df.iloc[i]['description']
    
    return base_strats

def get_editable_params(base_strategy_id):
    '''Get all editable parameters and their allowed values for the given base strategy ids
        VALID for Workbench API version 1 (as needs all param values combination to be valid
        Takes base_strategy_id as input
        Example of expected output :
        [
          {"id": 1, "name": "Rebalance Frequency", "allowed_values": [
              {"id": 1, "name": "Weekly"},
              {"id": 2, "name": "Monthly"},
              {"id": 3, "name": "Quarterly"}
            ]
          },
          {"id": 2, "name": "Risk Manager", "allowed_values": [
            {"id": 1, "name": "Risk-averse"},
            {"id": 2, "name": "Risk-moderate"},
            {"id": 3, "name": "Risk-prone"}
          ]},
          {"id": 3, "name": "Allocation_Method", "allowed_values": [
            {"id": 1, "name": "Single-sector-ETF-equity-only"},
            {"id": 2, "name": "Single-sector-ETF-bond-only"},
            {"id": 3, "name": "Equal-sector-ETF-all"},
            {"id": 4, "name": "60-40-ETF-all"}
          ]}
       ]
    '''
    db_connect()
    query = "SELECT editable_params FROM base_strategies WHERE id = '%s'" % base_strategy_id
    try:
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        editable_params = yaml.safe_load(rows[0]['editable_params'])
    except:
        sys.exit("Failed to fetch editable params for base strategy '%s'" % base_strategy_id)
    db_close()
    return editable_params

def get_ticker_for_strategy(base_strategy_id, param_id_to_value_id):
    # Returns something like {"name": "Fixed Allocation Strategy(ETFs)", ticker: "1234-some-ticker-name" }
    # TODO Will change this logic once tickers are inserted in DB, for now returning base_name shortcode with strat_id
    param_id_to_value_id_dict = {}
    for k,v in json.loads(param_id_to_value_id).items():
        param_id_to_value_id_dict[int(k)] = v
    paramid_valueid_hash = hashlib.md5(json.dumps(param_id_to_value_id_dict, sort_keys=True)).hexdigest()
    
    db_connect()
    query = "SELECT simulation_id as id FROM workbench_strategies WHERE base_strategy_id = '%s' AND paramid_valueid_hash = '%s'" %(base_strategy_id, paramid_valueid_hash)
    try:
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        ticker = rows[0]['id'] # TODO change this part when we have tickers implemented
    except:
        sys.exit("Failed to fetch ticker for selected variant of base strategy '%s'" % base_strategy_id)
    # Base strategy short code for now, will not need it as tickers will include this part going forward
    query = "SELECT shortcode, base_strategy FROM base_strategies WHERE id = '%s'" %(base_strategy_id)
    try:
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        shortcode = rows[0]['shortcode'] # TODO change this part when we have tickers implemented
        base_strategy = rows[0]['base_strategy'] # Name
    except:
        sys.exit("Failed to fetch ticker for selected variant of base strategy '%s'" % base_strategy_id)
    db_close()
    return { 'name' : base_strategy, 'ticker' : shortcode + str(ticker)}

# Function to get stats given base strategy id and dict of param_id - param_value_id
def get_stats_for_base_strategy(base_strategy_id, param_id_to_value_id):
    """Maps combination of parameters and base strategy to a strategy id
       and returns stats for that strategy
    Parameters:
        1. base_strategy_id(string)
        2. param_id_to_value_id (currently assumes dict, can be updated for json)
    Returns:
        All stats (dict)
        e.g. {'daily_log_returns':[0.1,0.2],'dates':['2015-05-05','2015-05-06'],'sharpe':1.5}
    """
    param_id_to_value_id_dict = {}
    for k,v in json.loads(param_id_to_value_id).items():
        param_id_to_value_id_dict[int(k)] = v
    paramid_valueid_hash = hashlib.md5(json.dumps(param_id_to_value_id_dict, sort_keys=True)).hexdigest()

    db_connect()
    query = "SELECT strat_id as id, sector_list FROM strategy_static AS a JOIN workbench_strategies AS b ON a.strat_id = b.simulation_id WHERE b.base_strategy_id = '%s' AND b.paramid_valueid_hash = '%s' AND b.is_taxable = False" %(base_strategy_id, paramid_valueid_hash)
    try:
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        ret_dict['id'] = rows[0]['id']
        sector_list = json.loads(rows[0]['sector_list'])
    except:
        sys.exit("Failed to fetch sectors for this variant of base strategy '%s'" % base_strategy_id)

    query = "SELECT a.strat_id as id, a.date as strategy_dates, a.daily_log_return as strategy_pretax_log_returns, a.daily_sector_weights, b.daily_log_return as strategy_posttax_log_returns FROM wb_strategies AS a JOIN workbench_strategies AS b on a.id = b.simulation_id WHERE b.base_strategy_id = '%s' AND b.paramid_valueid_hash = '%s'" %(base_strategy_id, paramid_valueid_hash)
    strategy_df = pd.read_sql(query, con=db)
    daily_weights = json.loads(strategy_df.iloc[0]['daily_weights'])
    dates = json.loads(strategy_df.iloc[0]['dates'])
    log_returns = json.loads(strategy_df.iloc[0]['daily_log_returns'])
    strategy_df['strategy_dates'] = [dates]

    strategy_df['strategy_pretax_log_returns'] = [ log_returns ]
    strategy_df['strategy_posttax_log_returns'] = [list( numpy.array(log_returns) * 0.7) ] # TODO

    #strategy_df['strategy_turnover'] = round( strategy_df['strategy_turnover'], 3 )
    #strategy_df['strategy_return'] = round( strategy_df['strategy_return'], 3 )
    #strategy_df['strategy_volatility'] = round( strategy_df['strategy_volatility'], 3)
    #strategy_df['strategy_drawdown'] = round( strategy_df['strategy_drawdown'], 3)
    #strategy_df['strategy_sharpe'] = round( strategy_df['strategy_sharpe'], 3)
    #strategy_df['strategy_return_to_drawdown'] = round( strategy_df['strategy_return_to_drawdown'], 3)

    query = "SELECT dates AS benchmark_dates, daily_log_returns AS benchmark_daily_log_returns FROM wb_strategies WHERE id = '1601'"
    strategy_df_1 = pd.read_sql(query, con=db)
    db_close()
    benchmark_daily_log_returns = json.loads(strategy_df_1.iloc[0]['benchmark_daily_log_returns'])
    benchmark_dates = json.loads(strategy_df_1.iloc[0]['benchmark_dates'])
    strategy_df['benchmark_name'] = "VTSMX"
    strategy_df['benchmark_dates'] = [ benchmark_dates ]
    strategy_df['benchmark_log_returns'] = [benchmark_daily_log_returns]
    #strategy_df['benchmark_percentage_cumulative_returns'] = [list( ( numpy.exp(numpy.array(benchmark_daily_log_returns).cumsum()) - 1 ) * 100.0) ]
    #strategy_df['benchmark_turnover'] = 90.0 #TODO
    #strategy_df['benchmark_return'] = round( strategy_df_1['benchmark_return'], 3 )
    #strategy_df['benchmark_volatility'] = round( strategy_df_1['benchmark_volatility'], 3)
    #strategy_df['benchmark_drawdown'] = round( strategy_df_1['benchmark_drawdown'], 3)
    #strategy_df['benchmark_sharpe'] = round( strategy_df_1['benchmark_sharpe'], 3)
    #strategy_df['benchmark_return_to_drawdown'] = round( strategy_df_1['benchmark_return_to_drawdown'], 3)

    columns = ['strategy_pretax_log_returns', 'strategy_posttax_log_returns', 'benchmark_log_returns']
    
    ## Round to reduce precision and data being sent
    for c in columns:
        strategy_df[c] = [[round(x,5) for x in strategy_df.iloc[0][c]]]

    strategy_df = strategy_df.drop('daily_log_returns', 1)
    strategy_df = strategy_df.drop('daily_weights', 1)
    strategy_df = strategy_df.drop('dates', 1)

    ret_dict = strategy_df.iloc[0].to_dict()
    products = daily_weights[0]
    ret_dict['sector_allocation'] = {} #TODO
    alloc_sum = 0.0

    all_sectors = []
    for i, product in enumerate( products ):
        alloc_sum += abs( daily_weights[-1][i] )
        all_sectors.append( sector_map[product] )
    all_sectors = list(set(all_sectors))

    ret_dict['sector_allocation']['dates'] = []
    for sector in all_sectors:
        ret_dict['sector_allocation'][sector] = []

    for j in  xrange(1,len(dates)+1):
        alloc_sum = 0.0
        ret_dict['sector_allocation']['dates'].append( dates[j-1] )
        for sector in all_sectors:
            ret_dict['sector_allocation'][sector].append( 0.0 )
        
        for i, product in enumerate( products ):
            alloc_sum += abs( daily_weights[j][i] )
        for i, product in enumerate( products ):
            ret_dict['sector_allocation'][ sector_map[product] ][-1] += ( 100*daily_weights[j][i]/alloc_sum if alloc_sum != 0.0 else 0.0 )
        for sector in all_sectors:
            ret_dict['sector_allocation'][sector][-1] = round( ret_dict['sector_allocation'][sector][-1], 3 )
    return ret_dict

# Function to get stats given base strategy id and dict of param_id - param_value_id
def get_product_allocations_for_base_strategy(base_strategy_id, param_id_to_value_id, end_date):
    param_id_to_value_id_dict = {}
    for k,v in json.loads(param_id_to_value_id).items():
        param_id_to_value_id_dict[int(k)] = v
    paramid_valueid_hash = hashlib.md5(json.dumps(param_id_to_value_id_dict, sort_keys=True)).hexdigest()

    db_connect()
    query = "SELECT a.date, a.daily_weights, b.product_list FROM strategy_static AS b JOIN strategy_daily AS a on a.strat_id = b.strat_id JOIN workbench_strategies AS c on b.strat_id = c.simulation_id WHERE c.base_strategy_id = '%s' AND c.paramid_valueid_hash = '%s' AND a.date <= '%s' ORDER BY a.date DESC LIMIT 1" %(base_strategy_id, paramid_valueid_hash, end_date)
    product_df = pd.read_sql(query, con=db)
    db_close()
    daily_weights = json.loads(product_df.iloc[0]['daily_weights'])
    products = json.loads(product_df.iloc[0]['product_list'])

    product_dict = {}
    product_dict['date'] = product_df.iloc[0]['date'].strftime('%Y-%m-%d')
    product_dict['product_allocation'] = {}
    product_dict['product_sector'] = {}
    for idx in xrange(len(products)):
        product_dict['product_allocation'][products[idx]] = round( 100*daily_weights[idx], 3 )
        product_dict['product_sector'][products[idx]] = sector_map[products[idx]]
    return product_dict
