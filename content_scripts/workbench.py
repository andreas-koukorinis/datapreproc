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
    query = "SELECT simulation_id as id FROM workbench_strategies WHERE base_strategy_id = '%s' AND paramid_valueid_hash = '%s' AND is_taxable = False" %(base_strategy_id, paramid_valueid_hash)
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

    ret_dict = {}

    db_connect()
    query = "SELECT strat_id as id, sector_list FROM strategy_static AS a JOIN workbench_strategies AS b ON a.strat_id = b.simulation_id WHERE b.base_strategy_id = '%s' AND b.paramid_valueid_hash = '%s' AND b.is_taxable = False" %(base_strategy_id, paramid_valueid_hash)
    try:
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        ret_dict['id'] = rows[0]['id']
        pretax_id = rows[0]['id']
        sector_list = json.loads(rows[0]['sector_list'])
    except:
        sys.exit("Failed to fetch sectors for this variant of base strategy '%s'" % base_strategy_id)

    query = "SELECT simulation_id AS posttax_id FROM workbench_strategies WHERE is_taxable = True AND paramid_valueid_hash = '%s' AND base_strategy_id = '%s'" % (paramid_valueid_hash, base_strategy_id)
    try:
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        posttax_id = rows[0]['posttax_id']
    except:
        sys.exit("Failed to fetch post_tax variant for the desired parameters of base strategy '%s'" % base_strategy_id)

    query = "SELECT a.date as strategy_dates, a.daily_log_return as strategy_pretax_log_returns, a.daily_sector_weights, b.daily_log_return as strategy_posttax_log_returns FROM strategy_daily AS a JOIN strategy_daily AS b on a.strat_id = '%s' AND b.strat_id = '%s' AND a.date = b.date WHERE a.strat_id = '%s' AND b.strat_id = '%s'" %(pretax_id, posttax_id, pretax_id, posttax_id)
    strategy_df = pd.read_sql(query, con=db)
    ret_dict['strategy_dates'] = map(lambda x: x.strftime("%Y-%m-%d") , list(strategy_df['strategy_dates'].values))
    ret_dict['strategy_pretax_log_returns'] = list(numpy.round(strategy_df['strategy_pretax_log_returns'].values, 5))
    ret_dict['strategy_posttax_log_returns'] = list(numpy.round(strategy_df['strategy_posttax_log_returns'].values, 5))
    sector_weights = strategy_df['daily_sector_weights'].values
    ret_dict['sector_allocation'] = {}
    ret_dict['sector_allocation']['dates'] = map(lambda x: x.strftime("%Y-%m-%d") , list(strategy_df['strategy_dates'].values))
    num_sectors = len(sector_list)
    for sector in sector_list:
        ret_dict['sector_allocation'][sector] = [] # For each sector, we keep a list of sector allocation for dates
    for record in sector_weights:
        sector_value = json.loads(record)
        for idx in xrange(num_sectors):
            ret_dict['sector_allocation'][sector_list[idx]].append(round(sector_value[idx], 3))

    # Get benchmark for base strategy
    query = "SELECT benchmark FROM base_strategies WHERE id = '%s'" % base_strategy_id
    try:
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        benchmark = rows[0]['benchmark']
    except:
        sys.exit("Failed to fetch benchmark for this variant of base strategy '%s'" % base_strategy_id)

    # Handling 60-40 combination with hard-coding now, will change once logic is finalized
    # Handling of different frequency of reported returns will be handled after finalizing benchmark combination
    # Combination benchmark assumes daily rebalancing for now
    if benchmark == '60S40B':
        equity_benchmark = 'VTSMX'
        bond_benchmark = 'VBLTX'
        ret_dict['benchmark_name'] = '60% - ' + equity_benchmark + '& 40% - ' + bond_benchmark
        query = "SELECT a.date, 0.6 * a.logreturn + 0.4 * b.logreturn AS log_return FROM benchmark_daily AS a LEFT JOIN benchmark_daily AS b ON a.date = b.date AND a.ticker = '%s' AND b.ticker = '%s' WHERE a.ticker = '%s' AND b.ticker = '%s' UNION SELECT b.date, 0.6 * a.logreturn + 0.4 * b.logreturn AS log_return FROM benchmark_daily AS a RIGHT JOIN benchmark_daily AS b ON a.date = b.date AND a.ticker = '%s' AND b.ticker = '%s' WHERE a.ticker = '%s' AND b.ticker = '%s'" % (equity_benchmark, bond_benchmark, equity_benchmark, bond_benchmark, equity_benchmark, bond_benchmark, equity_benchmark, bond_benchmark)
    else:
        ret_dict['benchmark_name'] = benchmark
        query = "SELECT date, logreturn AS log_return FROM benchmark_daily WHERE ticker = '%s'" % benchmark
    benchmark_df = pd.read_sql(query, con=db)
    db_close()
    ret_dict['benchmark_dates'] = map(lambda x: x.strftime("%Y-%m-%d") , list(benchmark_df['date'].values))
    #benchmark_df['benchmark_turnover'] = 90.0 #TODO

    ## Round to reduce precision and data being sent
    ret_dict['benchmark_log_returns'] = list(numpy.round(benchmark_df['log_return'].values, 5))

    return ret_dict

# Function to get stats given base strategy id and dict of param_id - param_value_id
def get_product_allocations_for_base_strategy(base_strategy_id, param_id_to_value_id, end_date):
    param_id_to_value_id_dict = {}
    for k,v in json.loads(param_id_to_value_id).items():
        param_id_to_value_id_dict[int(k)] = v
    paramid_valueid_hash = hashlib.md5(json.dumps(param_id_to_value_id_dict, sort_keys=True)).hexdigest()

    db_connect()
    query = "SELECT a.date, a.daily_weights, b.product_list FROM strategy_static AS b JOIN strategy_daily AS a on a.strat_id = b.strat_id JOIN workbench_strategies AS c on b.strat_id = c.simulation_id WHERE c.base_strategy_id = '%s' AND c.paramid_valueid_hash = '%s' AND c.is_taxable = False AND a.date <= '%s' ORDER BY a.date DESC LIMIT 1" %(base_strategy_id, paramid_valueid_hash, end_date)
    product_df = pd.read_sql(query, con=db)
    db_close()
    daily_weights = json.loads(product_df.iloc[0]['daily_weights'])
    products = json.loads(product_df.iloc[0]['product_list'])

    product_dict = {}
    product_dict['product_sector'] = {}
    for idx in xrange(len(products)):
        product_dict['product_allocation'][products[idx]] = round( 100*daily_weights[idx], 3 )
        product_dict['product_sector'][products[idx]] = sector_map[products[idx]]
    return product_dict
