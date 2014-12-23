import sys
import datetime
from Utils import defaults
from risk_manager_algorithm import RiskManagerAlgo

class VarLevel(object):
    '''Variables that encapsulate current var level'''
    def __init__(self, _var_level, _max_historical_val):
        self.var_level = _var_level
        self.max_historical_val = _max_historical_val

class RiskManagerVar(RiskManagerAlgo):
    '''In this risk manager algorithm, we use var as a measure of risk level.(not capital allocation)
       When the strategy hits a higher drawdown/stoploss level the maximum var allowed to the strategy is reduced.
       Then an estimate of the current(actually future) var level of the strategy is calculated based on daily rebalanced CWAS
       on the current weights(using simple performance tracker).Now, given the max_var_level_allowed and current_var_level we 
       calculate the fraction of the current_portfolio_value to be allocated to the strategy i.e. min(1.0, max_var_allowed/current_var_level)
    
       Args:
       var_levels=2.0,1.5,1.0,0.5,0
       drawdown_levels=15,20,25,30
       stoploss_levels=10,15,20,25
       return_history=252
    '''

