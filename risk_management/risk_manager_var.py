import sys
import datetime
from Utils import defaults
from risk_manager_algorithm import RiskManagerAlgo

class VarLevel(object):
    '''Variables that encapsulate current var level'''
    def __init__(self, _var_level, _max_historical_drawdown, _max_historical_stoploss):
        self.var_level = _var_level
        self.max_historical_drawdown = _max_historical_drawdown
        self.max_historical_stoploss = _max_historical_stoploss

class RiskManagerVar(RiskManagerAlgo):
    '''In this risk manager algorithm, we use var as a measure of risk level.(not capital allocation)
       When the strategy hits a higher drawdown/stoploss level the maximum var allowed to the strategy is reduced.
       Then an estimate of the current(actually future) var level of the strategy is calculated based on daily rebalanced CWAS
       on the current weights(using simple performance tracker).Now, given the max_var_level_allowed and current_var_level we 
       calculate the fraction of the current_portfolio_value to be allocated to the strategy i.e. min(1.0, max_var_allowed/current_var_level)
    
       Args:
           var_levels=2.0,1.5,1.0,0
           drawdown_levels=10,15,20
           stoploss_levels=10,15,20
           return_history=252
           var_computation_interval=63
    '''

    def init(self, _config):
        """Read the parameters: var_levels, drawdown_levels, stoploss_levels, return_history and setup var_level_vec
        
        Args:
            _config(ConfigParser handle): Handle to the config file

        Returns: Nothing
        """

        self.return_history = int(defaults.RETURN_HISTORY)
        if _config.has_option('RiskManagement', 'return_history'):
            self.return_history = _config.getint('RiskManagement', 'return_history')

        self.var_computation_interval = int(defaults.VAR_COMPUTATION_INTERVAL)
        if _config.has_option('RiskManagement', 'var_computation_interval'):
            self.var_computation_interval = _config.getint('RiskManagement', 'var_computation_interval')

        _drawdown_levels = list(defaults.DRAWDOWN_LEVELS)
        if _config.has_option('RiskManagement', 'drawdown_levels'):
            _drawdown_levels = _config.get('RiskManagement', 'drawdown_levels').split(',')
        self.drawdown_levels = ([float(x) for x in _drawdown_levels])
        for i in xrange(1, len(self.drawdown_levels)): #check that drawdown levels are in increasing order
            if self.drawdown_levels[i-1] >= self.drawdown_levels[i]:
                sys.exit("Drawdown levels should be in increasing order. They seem to not be so! %s" %(','.join(_drawdown_levels)))

        _stoploss_levels = list(defaults.STOPLOSS_LEVELS)
        if _config.has_option('RiskManagement', 'stoploss_levels'):
            _stoploss_levels = _config.get('RiskManagement', 'stoploss_levels').split(',')
        self.stoploss_levels = ([float(x) for x in _stoploss_levels])
        for i in xrange(1, len(self.stoploss_levels)): #check that stoploss levels are in increasing order
            if self.stoploss_levels[i-1] >= self.stoploss_levels[i]:
                sys.exit("Stoploss levels should be in increasing order. They seem to not be so! %s" %(','.join(_drawdown_levels)))

        _var_levels = list(defaults.VAR_LEVELS)
        if _config.has_option('RiskManagement', 'var_levels'):
            _var_levels = _config.get('RiskManagement', 'var_levels').split(',')
        _var_level_vec = [float(x) for x in _var_levels]
        #check that the last var level is 0.0 and first var level is 2.0
        if _var_level_vec[0] < 2.0:
            _var_level_vec.insert(0, 2.0) # Highest var level is 2.0
        if _var_level_vec[-1] > 0.0:
            _var_level_vec.append(0.0) # Liquidate on last level
        #check that the var levels are in descending order
        for i in xrange (1, len(_var_level_vec)):
            if _var_level_vec[i-1] <= _var_level_vec[i]:
                sys.exit("Var levels should be in increasing order. They seem to not be so! %s" %(','.join(_var_levels)))

        #check that the length of var_level_vec is 1 greater than the length of drawdown_levels, stoploss_levels
        if len(_var_level_vec) != 1 + len(_drawdown_levels) or len(_var_level_vec) != 1 + len(_stoploss_levels):
            sys.exit("Number of var levels should be 1 greater than the number of drawdown/stoploss levels.Does not hold!")

        #setup the var level array
        self.var_level_vec = []
        _current_max_drawdown = 100
        _current_max_stoploss = 100
        for i in xrange(0,len(_var_level_vec)):
            _var_level = _var_level_vec[i]
            if i < len(self.drawdown_levels):
                _current_max_drawdown = self.drawdown_levels[i]
            else:
                _current_max_drawdown = 100
            if i < len(self.stoploss_levels):
                _current_max_stoploss = self.stoploss_levels[i]
            else:
                _current_max_stoploss = 100
            _this_var_level = VarLevel(_var_level, _current_max_drawdown, _current_max_stoploss)
            self.var_level_vec.append(_this_var_level)

        self.current_var_level_index = 0 # highest var level. Unfortunately the highest var level has the smallest index
        self.current_var_estimate = 100.0 # Highest value
        self.current_capital_allocation_level = 100.0 # Fully allocated initially
        self.last_risk_level_updated_date = datetime.datetime.fromtimestamp(0).date()
        self.last_var_computation_date = datetime.datetime.fromtimestamp(0).date()

    def get_var_level_index_from_drawdown(self,_current_drawdown):
        """Private function to compute the correct index of the var level array based on the current drawdown level
           
        Args:
            _current_drawdown: The current drawdown of the strategy as calculated by the simple performance tracker

        Returns: the correct index of the var level array based on the current drawdown level
        """
        _retindex = len(self.var_level_vec)-1
        # start at lowest var level and keep trying a higher var level as long as the max drawdown of that level will be higher 
        while (_retindex > 0) and (self.var_level_vec[(_retindex - 1)].max_historical_drawdown > _current_drawdown):
            _retindex = _retindex - 1
        return (_retindex) # _retindex cannot be < 0 and cannot be _retindex cannot be >= len(self.risk_level_vec). Hence _retindex is a valid index to the array

    def get_var_level_index_from_stoploss(self, _current_loss):
        """Private function to compute the correct index of the var level array based on the current loss level
           
        Args:
            _current_loss: The current loss(% of initial capital) of the strategy as calculated by the simple performance tracker

        Returns: the correct index of the var level array based on the current loss level
        """
        _retindex = len(self.var_level_vec)-1
        while (_retindex > 0) and (self.var_level_vec[(_retindex - 1)].max_historical_stoploss > _current_loss):
            _retindex = _retindex - 1
        return (_retindex)

    def get_current_risk_level(self, _date):
        """Public function, called by execlogic to compute the capital allocation level
           This function first calculated the max_allowed_VAR based on the stoploss/drawdown levels
           Then it updates the estimated value of the VAR of the strategy
           Finally it computes the fraction of capital to be allocated to have a VAR <= the max_allowed_VAR
        
        Args:
            _date: The date on which the risk level is being computed 

        Returns: The current risk level(% of capital to be allocated)
        """
        # first check if we have already done the computation today. If so then return previously computed value.
        if self.last_risk_level_updated_date == _date:
            return self.current_capital_allocation_level
        else:
            self.last_risk_level_updated_date = _date

        # get current drawdown from performance tracker
        _current_drawdown = self.simple_performance_tracker.get_current_drawdown()
        _current_loss = self.simple_performance_tracker.get_current_loss()
        _target_var_level_index = self.get_var_level_index_from_drawdown(_current_drawdown)
        _change_reason = 'Drawdown %0.2f%%' % _current_drawdown
        _stoploss_var_level_index = self.get_var_level_index_from_stoploss(_current_loss)
        if _target_var_level_index < _stoploss_var_level_index: # Take max of stoploss-var-index and drawdown-var-index
            _change_reason = 'Stoploss %0.2f%%' % _current_loss
            _target_var_level_index = _stoploss_var_level_index
        if _target_var_level_index != self.current_var_level_index:
            print "On %s Max allowed VAR changed from %0.2f to %0.2f due to %s" % (_date, self.var_level_vec[self.current_var_level_index].var_level, self.var_level_vec[_target_var_level_index].var_level, _change_reason)
            self.current_var_level_index = _target_var_level_index    

        if self.last_var_computation_date + datetime.timedelta(days=self.var_computation_interval) <= _date:
            self.last_var_computation_date = _date
            self.current_var_estimate = self.simple_performance_tracker.compute_current_var_estimate(self.return_history)

        _new_captial_allocation = 100.0 * min(1.0, self.var_level_vec[_target_var_level_index].var_level/self.current_var_estimate)
        if _new_captial_allocation != self.current_capital_allocation_level:
            self.current_capital_allocation_level = _new_captial_allocation
            self.issue_notification_capital_update(_date, 'VAR', self.current_capital_allocation_level)
        return self.current_capital_allocation_level
