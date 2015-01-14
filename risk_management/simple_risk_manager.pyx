# cython: profile=True
import sys
from risk_manager_algorithm import RiskManagerAlgo
from utils import defaults
from utils.regular import adjust_file_path_for_home_directory

class SimpleRiskManager(RiskManagerAlgo):
    '''In this risk manager deallcation of capital is based on stoploss levels, drawdown levels, maxloss, and max trading cost per year
       The reallocation of deallocated capital is based on the paper returns computed using the simple performance tracker on daily rebalanced CWAS
       If we stopped out due to max trading cost, we do not restart again
       Parameters:
       stoploss_levels=50,70,100
       drawdown_levels=12,20,30
       maxloss=100
       max_trading_cost=2
       capital_allocation_levels=50,25
       reallocation_returns=15,10,5
       return_history=63'''
 
    def init(self, _config):
       #Assign default values
       self.stoploss_levels = [50.0, 70.0, 100.0]
       self.drawdown_levels = [12.0, 20.0, 30.0]
       self.maxloss = 100.0
       self.max_trading_cost = 2.0
       self.capital_allocation_levels = [50.0, 25.0]
       self.reallocation_returns = [15.0, 10.0, 5.0]
       self.return_history = 63

       # Load existing values from riskprofile_file 
       _riskprofilefilepath = "/dev/null"
       if _config.has_option('RiskManagement', 'risk_profile'):
           _riskprofilefilepath = adjust_file_path_for_home_directory(_config.get('RiskManagement', 'risk_profile'))
       self.process_riskprofile_file(_riskprofilefilepath)

       if self.capital_allocation_levels[0] < 100: # Append 100 to the front
           self.capital_allocation_levels.insert(0, 100.0)
       if self.capital_allocation_levels[-1] > 1: # Append 0 to the end
           self.capital_allocation_levels.append(0.0)
         
       # Check the values
       for i in xrange(1, len(self.stoploss_levels)): #check that stoploss levels are in increasing order
           if self.stoploss_levels[i-1] > self.stoploss_levels[i]:
               sys.exit("Stoploss levels should be in increasing order. They seem to not be so! %s" %(','.join(self.stoploss_levels)))
       for i in xrange(1, len(self.drawdown_levels)): #check that drawdown levels are in increasing order
           if self.drawdown_levels[i-1] > self.drawdown_levels[i]:
               sys.exit("Drawdown levels should be in increasing order. They seem to not be so! %s" %(','.join(self.drawdown_levels)))
       for i in xrange(1, len(self.capital_allocation_levels)): #check that capital_allocation levels are in decreasing order
           if self.capital_allocation_levels[i-1] < self.capital_allocation_levels[i]:
               sys.exit("Capital_allocation levels should be in decreasing order. They seem to not be so! %s" %(','.join(self.capital_allocation_levels)))
       for i in xrange(1, len(self.reallocation_returns)): #check that reallocation returns levels are in decreasing order
           if self.reallocation_returns[i-1] < self.reallocation_returns[i]:
               sys.exit("Reallocation returns levels should be in decreasing order. They seem to not be so! %s" %(','.join(self.reallocation_returns)))
       if len(self.capital_allocation_levels) != 1 + len(self.drawdown_levels) or len(self.capital_allocation_levels) != 1 + len(self.stoploss_levels) or len(self.capital_allocation_levels) != 1 + len(self.reallocation_returns):
           sys.exit("Number of capital_allocation_levels should be 1 greater than the number of drawdown/stoploss/reallocation levels.Does not hold!")

       self.max_trading_cost = self.performance_tracker.initial_capital * self.max_trading_cost/100.0
       # Fully allocated initially
       self.current_capital_allocation_level = 100.0

    def process_riskprofile_file(self, _modelfilepath):
        """Process the riskprofile_file and load the values
  
        Args:
            _modelfilepath: the path to the risk profile file

        Returns: Nothing
        """
        _model_file_handle = open(_modelfilepath, "r")
        # We expect lines like:
        # stoploss_levels 50 70 100
        # drawdown_levels 12 20 30
        # maxloss 100
        # max_trading_cost 2
        # capital_allocation_levels 50 25
        # reallocation_returns 15 10 5
        # return_history 63 
        for _model_line in _model_file_handle:
            _model_line_words = _model_line.strip().split(' ')
            if len(_model_line_words) >= 2:
                if _model_line_words[0] == 'stoploss_levels':
                    self.stoploss_levels = [float(x) for x in _model_line_words[1:]]
                elif _model_line_words[0] == 'drawdown_levels':
                    self.drawdown_levels = [float(x) for x in _model_line_words[1:]]
                elif _model_line_words[0] == 'maxloss':
                    self.maxloss = float(_model_line_words[1])
                elif _model_line_words[0] == 'max_trading_cost':
                    self.max_trading_cost = float(_model_line_words[1])
                elif _model_line_words[0] == 'capital_allocation_levels':
                    self.capital_allocation_levels = [float(x) for x in _model_line_words[1:]]
                elif _model_line_words[0] == 'reallocation_returns':
                    self.reallocation_returns = [float(x) for x in _model_line_words[1:]]
                elif _model_line_words[0] == 'return_history':
                    self.return_history = int(_model_line_words[1])
          
    def get_current_risk_level(self, _date):
        if self.last_risk_level_updated_date == _date:
            return (self.current_capital_allocation_level)
        else:
            self.last_risk_level_updated_date = _date
        if _date.year == self.performance_tracker.current_year_trading_cost[0]:
            _current_year_trading_cost = self.performance_tracker.current_year_trading_cost[1]
        else:
            _current_year_trading_cost = 0.0 #TODO change to transaction cost from simple_performance_tracker
        _current_loss = self.simple_performance_tracker.current_loss
        _current_drawdown = self.simple_performance_tracker.current_drawdown
        _new_allocation_level = self.current_capital_allocation_level
        _change_reason = ''

        # DEALLOCATION
        for i in range(len(self.stoploss_levels) - 1, -1, -1): #Stoploss
            if _current_loss > self.stoploss_levels[i]:
                if self.stoploss_flag != i:
                    # We have entered this level for the first time.
                    self.issue_notification_level_update(_date, 'StopLoss', _current_loss, self.stoploss_levels[i])
                if _new_allocation_level > self.capital_allocation_levels[i+1]:
                    _new_allocation_level = self.capital_allocation_levels[i+1]
                    _change_reason = 'Stoploss'
                self.stoploss_flag = i
                break # We break since we are starting from the worst level. Hence future inequalities
        if _current_loss < self.stoploss_levels[0]:
            self.stoploss_flag = -1

        for i in range(len(self.drawdown_levels) - 1, -1, -1): 
            if _current_drawdown > self.drawdown_levels[i]: #Drawdown
                if self.drawdown_flag != i:
                    self.issue_notification_level_update(_date, 'Drawdown', _current_drawdown, self.drawdown_levels[i])
                if _new_allocation_level > self.capital_allocation_levels[i+1]:
                    _new_allocation_level = self.capital_allocation_levels[i+1]
                    _change_reason = 'Drawdown'
                self.drawdown_flag = i
                break
        if _current_drawdown < self.drawdown_levels[0]:
            self.drawdown_flag = -1

        if _current_loss > self.maxloss: #Maxloss
            if self.maxloss_flag != 0:
                self.issue_notification_level_update(_date, 'Maxloss', _current_loss, self.maxloss)
            self.maxloss_flag = 0
            _new_allocation_level = self.capital_allocation_levels[-1]
            _change_reason = 'Maxloss'
        else:
            self.maxloss_flag = -1

        # If we reach the max trading cost then stop trading and liquidate the portfolio
        if _current_year_trading_cost > self.max_trading_cost: #MaxTradingCost
            if self.max_trading_cost_flag != 0:
                self.issue_notification_level_update(_date, 'Trading Cost', _current_year_trading_cost, self.max_trading_cost)
            self.max_trading_cost_flag = 0
            _change_reason = 'Trading Cost'
            _new_allocation_level = self.capital_allocation_levels[-1]

        # REALLOCATION
        # If not fully invested, check the paper returns and reallocate the capital accordingly
        # print 'current allocation level %f'%self.current_capital_allocation_level
        if _new_allocation_level < 100.0 and self.max_trading_cost_flag == -1: # If not fully allocated and did not reach max trading cost earlier
            _paper_returns = self.simple_performance_tracker.compute_paper_returns(self.return_history)
            for i in range(0,len(self.reallocation_returns)): # If we have had good enough returns in the past and current allocation is less than desired
                if _paper_returns >= self.reallocation_returns[i]:
                    _desired_allocation = self.capital_allocation_levels[i]
                    if _new_allocation_level < _desired_allocation:
                        _new_allocation_level = _desired_allocation
                        _change_reason = 'Reallocated'
                    break #start at the highest level. Hence we break.
        if _new_allocation_level != self.current_capital_allocation_level:
            self.issue_notification_capital_update(_date, _change_reason, _new_allocation_level)
            self.current_capital_allocation_level = _new_allocation_level

        _retval = self.current_capital_allocation_level
        # if self.simple_performance_tracker.get_desired_leverage() > self.maximum_allowed_leverage:
        #     print ("didn't expect to see desired leverage %f to exceed maximum allowed leverage %f" %(self.simple_performance_tracker.get_desired_leverage(), self.maximum_allowed_leverage))
        #     _retval = min (_retval, (self.maximum_allowed_leverage/self.simple_performance_tracker.get_desired_leverage()))
        return (_retval)
