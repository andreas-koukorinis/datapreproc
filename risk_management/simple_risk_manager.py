from risk_manager_algorithm import RiskManagerAlgo
import numpy as np

class SimpleRiskManager(RiskManagerAlgo):
    '''Parameters:
       stoploss_levels=10,15,20
       drawdown_levels=10,15,20
       maxloss=30
       max_trading_cost=2
       capital_allocation_levels=70,40,0
       reallocation_returns=3,4,5
       return_history=126'''
 
    def init(self, _config):
        _stoploss_levels = _config.get('RiskManagement', 'stoploss_levels').split(',')
        self.stoploss_levels = sorted([float(x) for x in _stoploss_levels])
        _drawdown_levels = _config.get('RiskManagement', 'drawdown_levels').split(',')
        self.drawdown_levels = sorted([float(x) for x in _drawdown_levels])
        self.maxloss = _config.getfloat('RiskManagement', 'maxloss') 
        self.max_trading_cost = self.performance_tracker.initial_capital*_config.getfloat('RiskManagement', 'max_trading_cost')/100.0
        _capital_allocation_levels = _config.get('RiskManagement', 'capital_allocation_levels').split(',')
        self.capital_allocation_levels = sorted([float(x) for x in _capital_allocation_levels])
        _reallocation_returns = _config.get('RiskManagement', 'reallocation_returns').split(',')
        self.reallocation_returns = sorted([float(x) for x in _reallocation_returns])
        self.return_history = _config.getfloat('RiskManagement', 'return_history')
        self.current_allocation_level = 100.0 # Fully allocated initially

    def get_current_risk_level(self, date): 
        if date.year == self.performance_tracker.current_year_trading_cost[0]:
            _current_year_trading_cost = self.performance_tracker.current_year_trading_cost[1]
        else:
            _current_year_trading_cost = 0.0 #TODO change to transaction cost from simple_performance_tracker
        _current_loss = self.simple_performance_tracker.current_loss
        _current_drawdown = self.simple_performance_tracker.current_drawdown
        _updated = False
        #print date, self.simple_performance_tracker.current_loss, self.simple_performance_tracker.current_drawdown, self.performance_tracker.current_loss, self.performance_tracker.current_drawdown, (np.exp(self.simple_performance_tracker.net_log_return)-1)*100.0, (np.exp(self.performance_tracker.net_log_return)-1)*100.0
        # Find out the appropriate allocation level using current_loss, current_drawdown and current_year_trading_cost 
        for i in range(len(self.capital_allocation_levels) - 1, -1, -1):
            if _current_loss > self.stoploss_levels[i]:
                self.issue_notification_level_update(date, 'StopLoss', _current_loss, self.stoploss_levels[i])
                self.issue_notification_capital_update(date, 'StopLoss', self.capital_allocation_levels[i])
                self.current_allocation_level = self.capital_allocation_levels[i]
                self.stoploss_flag = i
                _updated = True
            if _current_drawdown > self.drawdown_levels[i]:
                self.issue_notification_level_update(date, 'Drawdown', _current_drawdown, self.drawdown_levels[i])
                self.issue_notification_capital_update(date, 'Drawdown', self.capital_allocation_levels[i])
                self.current_allocation_level = self.capital_allocation_levels[i]
                self.drawdown_flag = i
                _updated = True
            if _updated:
                break
        if _current_loss > self.maxloss:
            self.current_allocation_level = self.capital_allocation_levels[-1]
            self.issue_notification_level_update(date, 'Maxloss', _current_loss, self.maxloss)
            self.issue_notification_capital_update(date, 'Maxloss', self.capital_allocation_levels[-1])
            self.maxloss_flag = 0
        if _current_year_trading_cost > self.max_trading_cost:
            self.current_allocation_level = self.capital_allocation_levels[-1]
            self.issue_notification_level_update(date, 'Trading Cost', _current_year_trading_cost, self.max_trading_cost)
            self.issue_notification_capital_update(date, 'Trading Cost', self.capital_allocation_levels[-1])
            self.max_trading_cost_flag = 0
        # If not fully invested, check the paper returns and reallocate the capital accordingly
        if self.current_allocation_level < 1.0: # If not fully allocated
            _paper_returns = self.compute_paper_returns(self.return_history)
            for i in range(len(self.reallocation_returns) - 1, -1, -1):
                if _paper_returns >= self.reallocation_returns[i] and i < self.current_allocation_level: # If we have had good enough returns in the past
                    self.current_allocation_level = self.capital_allocation_levels[i]
                    self.issue_notification_capital_update(date, 'Reallocated', self.capital_allocation_levels[i])
        return self.current_allocation_level

    def compute_paper_returns(self, return_history):
        _log_returns = self.simple_performance_tracker.log_return_history
        _start_index = max(0, _log_returns.shape[0] - return_history)
        _net_paper_log_return = 0.0
        for i in range(_start_index, _log_returns.shape[0]):
            _net_paper_log_return += np.log(1 + self.weights*_log_returns[i])
        _net_paper_return = (np.exp(_net_paper_log_return) - 1)*100.0
        return _net_paper_return
