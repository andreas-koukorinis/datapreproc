# cython: profile=True
import sys
import datetime
from utils import defaults
from risk_manager_algorithm import RiskManagerAlgo
from utils.regular import adjust_file_path_for_home_directory

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

        # Defaults
        self.var_levels = [2.0, 1.5, 1.0, 0.0]
        self.drawdown_levels = [10.0, 15.0, 20.0]
        self.stoploss_levels = [10.0, 15.0, 20.0]
        self.return_history = 252
        self.var_computation_interval = 63

        # Load existing values from riskprofile_file 
        _riskprofilefilepath = "/dev/null"
        if _config.has_option('RiskManagement', 'risk_profile'):
            _riskprofilefilepath = adjust_file_path_for_home_directory(_config.get('RiskManagement', 'risk_profile'))
        self.process_riskprofile_file(_riskprofilefilepath)
        
        for i in xrange(1, len(self.drawdown_levels)): #check that drawdown levels are in increasing order
            if self.drawdown_levels[i-1] > self.drawdown_levels[i]:
                sys.exit("Drawdown levels should be in increasing order. They seem to not be so! %s" %(','.join(self.drawdown_levels)))

        for i in xrange(1, len(self.stoploss_levels)): #check that stoploss levels are in increasing order
            if self.stoploss_levels[i-1] > self.stoploss_levels[i]:
                sys.exit("Stoploss levels should be in increasing order. They seem to not be so! %s" %(','.join(self.stoploss_levels)))

        #check that the last var level is 0.0 and first var level is 2.0
        if self.var_levels[0] < 2.0:
            self.var_levels.insert(0, 2.0) # Highest var level is 2.0
        if self.var_levels[-1] > 0.0:
            self.var_levels.append(0.0) # Liquidate on last level
        #check that the var levels are in descending order
        for i in xrange (1, len(self.var_levels)):
            if self.var_levels[i-1] <= self.var_levels[i]:
                sys.exit("Var levels should be in increasing order. They seem to not be so! %s" %(','.join(self.var_levels)))

        #check that the length of var_level_vec is 1 greater than the length of drawdown_levels, stoploss_levels
        if len(self.var_levels) != 1 + len(self.drawdown_levels) or len(self.var_levels) != 1 + len(self.stoploss_levels):
            sys.exit("Number of var levels should be 1 greater than the number of drawdown/stoploss levels.Does not hold!")

        #setup the var level array
        self.var_level_vec = []
        _current_max_drawdown = 100
        _current_max_stoploss = 100
        for i in xrange(0,len(self.var_levels)):
            _var_level = self.var_levels[i]
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

    def process_riskprofile_file(self, _modelfilepath):
        """Process the riskprofile_file and load the values

        Args:
            _modelfilepath: the path to the risk profile file

        Returns: Nothing
        """
        _model_file_handle = open(_modelfilepath, "r")
        # We expect lines like:
        # var_levels 2.0 1.5 1.0 0.0
        # drawdown_levels 10 15 20
        # stoploss_levels 10 15 20
        # return_history 252
        # var_computation_interval 63
        for _model_line in _model_file_handle:
            _model_line_words = _model_line.strip().split(' ')
            if len(_model_line_words) >= 2:
                if _model_line_words[0] == 'var_levels':
                    self.var_levels = [float(x) for x in _model_line_words[1:]]
                elif _model_line_words[0] == 'drawdown_levels':
                    self.drawdown_levels = [float(x) for x in _model_line_words[1:]]
                elif _model_line_words[0] == 'stoploss_levels':
                    self.stoploss_levels = [float(x) for x in _model_line_words[1:]]
                elif _model_line_words[0] == 'var_computation_interval':
                    self.var_computation_interval = float(_model_line_words[1])
                elif _model_line_words[0] == 'return_history':
                    self.return_history = float(_model_line_words[1])

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
            return (self.current_capital_allocation_level)
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
            #self.issue_notification_capital_update(_date, 'VAR', self.current_capital_allocation_level)

        _retval = self.current_capital_allocation_level
        # if self.simple_performance_tracker.get_desired_leverage() > self.maximum_allowed_leverage:
        #     print ("didn't expect to see desired leverage %f to exceed maximum allowed leverage %f" %(self.simple_performance_tracker.get_desired_leverage(), self.maximum_allowed_leverage))
        #     _retval = min (_retval, (self.maximum_allowed_leverage/self.simple_performance_tracker.get_desired_leverage()))
        return (_retval)
