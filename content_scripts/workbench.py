#!/usr/bin/env python 
"""API for building workbench"""

def get_base_strategies():
    """Get base stratefy ids and descriptions
    Returns:
        Base strategy ids and their correspoding descriptions(dict)
        e.g. {'12':'MVO','134':'TRVP','123':'SMS'}
    """

def get_params_for_base_strategy(base_strategy_id):
    """Get available parameter combinations for that strategy
    Parameters:
        1. base_strategy_id(string)
    Returns:
        All permissible sets of parameter configurations for a selected base strategy(List of dicts)
        e.g. [{'average_discretized_trend':21, 'std_lookback':21},
              {'average_discretized_trend':252, 'std_lookback':252},
              {'average_discretized_trend':[21, 63, 252], 'std_lookback':63}]
    """

def get_stats_for_strategy(base_strategy_id, chosen_params):
    """Maps combination of parameters and base strategy to a strategy id
       and returns stats for that strategy
    Parameters:
        1. base_strategy_id(string)
        2. chosen_params(dict)
        Keys should correspond to given base_strategy
    Returns:
        All stats (dict)
        e.g. {'daily_log_returns':[0.1,0.2],'dates':['2015-05-05','2015-05-06'],'sharpe':1.5}
    """"