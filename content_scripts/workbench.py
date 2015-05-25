#!/usr/bin/env python 
"""API for building workbench"""

def get_base_strategies():
    """Get base stratefy ids and descriptions
    Returns:
        Base strategy ids and their correspoding descriptions(dict)
        e.g. {'12':'MVO','134':'TRVP','123':'SMS'}
    """
    base_strategies = {'1':'Fixed Allocation Strategy', '2':'Risk Aware Allocation Strategy',\
                       '3':'Correlation and Risk Aware Allocation Strategy', '4':'Equalized Risk Allocation Strategy',\
                       '5':'Equalized Risk Momentum Strategy', '6':'Variance Aware Momentum Strategy',\
                       '7':'Discretized Momentum Strategy', '8':'VIX Futures based Carry Strategy'}
    return base_strategies

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
    cwas_params = ['product_type', 'leverage', 'rebalancing_frequency']
    cwas_combns = [{cwas_params[0]:'futures', cwas_params[1]:5, cwas_params[2]:21}, \
                   {cwas_params[0]:'futures', cwas_params[1]:5, cwas_params[2]:252}, \
                   {cwas_params[0]:'etfs',  cwas_params[2]:21}, \
                   {cwas_params[0]:'etfs',  cwas_params[2]:252}, \
                  ]

    trvp_params = ['product_type', 'target_risk', 'average_std_lookback', 'rebalancing_frequency']
    trvp_combns = [{trvp_params[0]:'futures', trvp_params[1]:10, trvp_params[2]:21, trvp_params[3]:21}, \
                   {trvp_params[0]:'futures', trvp_params[1]:10, trvp_params[2]:21, trvp_params[3]:126}, \
                   {trvp_params[0]:'futures', trvp_params[1]:20, trvp_params[2]:21, trvp_params[3]:21}, \
                   {trvp_params[0]:'futures', trvp_params[1]:10, trvp_params[2]:[21, 63, 252], trvp_params[3]:21}, \
                   {trvp_params[0]:'etfs', trvp_params[2]:21, trvp_params[3]:21}, \
                   {trvp_params[0]:'etfs', trvp_params[2]:[21, 63, 252], trvp_params[3]:21}, \
                   {trvp_params[0]:'etfs', trvp_params[2]:21, trvp_params[3]:252}, \
                  ]
    
    trmshc_params = ['product_type', 'target_risk', 'average_std_lookback', 'rebalancing_frequency']
    trmshc_combns = [{trmshc_params[0]:'futures', trmshc_params[1]:10, trmshc_params[2]:21, trmshc_params[3]:21}, \
                     {trmshc_params[0]:'futures', trmshc_params[1]:10, trmshc_params[2]:21, trmshc_params[3]:126}, \
                     {trmshc_params[0]:'futures', trmshc_params[1]:20, trmshc_params[2]:21, trmshc_params[3]:21}, \
                     {trmshc_params[0]:'futures', trmshc_params[1]:10, trmshc_params[2]:[21, 63, 252], trmshc_params[3]:21}, \
                     {trmshc_params[0]:'etfs', trmshc_params[2]:21, trmshc_params[3]:21}, \
                     {trmshc_params[0]:'etfs', trmshc_params[2]:[21, 63, 252], trmshc_params[3]:21}, \
                     {trmshc_params[0]:'etfs', trmshc_params[2]:21, trmshc_params[3]:252}, \
                    ]
    
    trerc_params = ['product_type', 'target_risk', 'average_std_lookback', 'rebalancing_frequency']
    trerc_combns = [{trerc_params[0]:'futures', trerc_params[1]:10, trerc_params[2]:21, trerc_params[3]:21}, \
                     {trerc_params[0]:'futures', trerc_params[1]:10, trerc_params[2]:21, trerc_params[3]:126}, \
                     {trerc_params[0]:'futures', trerc_params[1]:20, trerc_params[2]:21, trerc_params[3]:21}, \
                     {trerc_params[0]:'futures', trerc_params[1]:10, trerc_params[2]:[21, 63, 252], trerc_params[3]:21}, \
                     {trerc_params[0]:'etfs', trerc_params[2]:21, trerc_params[3]:21}, \
                     {trerc_params[0]:'etfs', trerc_params[2]:[21, 63, 252], trerc_params[3]:21}, \
                     {trerc_params[0]:'etfs', trerc_params[2]:21, trerc_params[3]:252}, \
                    ]

    strat_to_combn_map = {'1':cwas_combns, '2':trvp_combns,}\
                          '3':trmshc_combns, '4':trerc_combns,\
                          '5':treec_combns, '6':mvo_combns,\
                          '7':sms_combns, '8':trvc_combns}

    return strat_to_combn_map[base_strategy_id]

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
    """
