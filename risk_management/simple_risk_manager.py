from risk_manager_algorithm import RiskManagerAlgo
from Utils import defaults

class SimpleRiskManager(RiskManagerAlgo):
    '''In this risk manager deallcation of capital is based on stoploss levels, drawdown levels, maxloss, and max trading cost per year
       The reallocation of deallocated capital is based on the paper returns computed using the simple performance tracker on daily rebalanced CWAS
       If we stopped out due to max trading cost, we do not restart again
       Parameters:
       stoploss_levels=10,15,20
       drawdown_levels=10,15,20
       maxloss=30
       max_trading_cost=2
       capital_allocation_levels=70,40,0
       reallocation_returns=3,4,5
       return_history=126'''
 
    def init(self, _config):
        _stoploss_levels = list(defaults.STOPLOSS_LEVELS)
        if _config.has_option('RiskManagement', 'stoploss_levels'):
            _stoploss_levels = _config.get('RiskManagement', 'stoploss_levels').split(',')
        self.stoploss_levels = ([float(x) for x in _stoploss_levels])
        _drawdown_levels = list(defaults.DRAWDOWN_LEVELS)
        if _config.has_option('RiskManagement', 'drawdown_levels'):
            _drawdown_levels = _config.get('RiskManagement', 'drawdown_levels').split(',')
        self.drawdown_levels = ([float(x) for x in _drawdown_levels])
        self.maxloss = float(defaults.MAXLOSS)
        if _config.has_option('RiskManagement', 'maxloss'):
            self.maxloss = _config.getfloat('RiskManagement', 'maxloss') 
        self.max_trading_cost = float(defaults.MAX_TRADING_COST)
        if _config.has_option('RiskManagement', 'max_trading_cost'):
            self.max_trading_cost = _config.getfloat('RiskManagement', 'max_trading_cost')
        self.max_trading_cost = self.performance_tracker.initial_capital * self.max_trading_cost/100.0
        _capital_allocation_levels = list(defaults.CAPITAL_ALLOCATION_LEVELS)
        if _config.has_option('RiskManagement', 'capital_allocation_levels'):
            _capital_allocation_levels = _config.get('RiskManagement', 'capital_allocation_levels').split(',')
        self.capital_allocation_levels = [float(x) for x in _capital_allocation_levels]
        self.capital_allocation_levels.append(0.0) #Liquidate on last level
        self.reallocation_returns = list(defaults.REALLOCATION_LEVELS)
        if _config.has_option('RiskManagement', 'reallocation_returns'):
            _reallocation_returns = _config.get('RiskManagement', 'reallocation_returns').split(',')
        self.reallocation_returns = ([float(x) for x in _reallocation_returns])
        self.return_history = int(defaults.RETURN_HISTORY)
        if _config.has_option('RiskManagement', 'return_history'):
            self.return_history = _config.getint('RiskManagement', 'return_history')
        self.current_allocation_level = 100.0 # Fully allocated initially

    def get_current_risk_level(self, _date):
        if self.last_risk_level_updated_date == _date:
            return self.current_allocation_level
        else:
            self.last_risk_level_updated_date = _date
        if _date.year == self.performance_tracker.current_year_trading_cost[0]:
            _current_year_trading_cost = self.performance_tracker.current_year_trading_cost[1]
        else:
            _current_year_trading_cost = 0.0 #TODO change to transaction cost from simple_performance_tracker
        _current_loss = self.simple_performance_tracker.current_loss
        _current_drawdown = self.simple_performance_tracker.current_drawdown

        # DEALLOCATION
        for i in range(len(self.capital_allocation_levels) - 1, -1, -1): #Stoploss
            if _current_loss > self.stoploss_levels[i]:
                if self.stoploss_flag != i:
                    # We have entered this level for the first time.
                    self.issue_notification_level_update(_date, 'StopLoss', _current_loss, self.stoploss_levels[i])
                if self.current_allocation_level > self.capital_allocation_levels[i]:
                    # We should have a capital allocation <= capital_allocation_levels[i]
                    self.issue_notification_capital_update(_date, 'StopLoss', self.capital_allocation_levels[i])
                    self.current_allocation_level = self.capital_allocation_levels[i]
                self.stoploss_flag = i
                break # We break since we are starting from the worst level. Hence future inequalities
        if _current_loss < self.stoploss_levels[0]:
            self.stoploss_flag = -1

        for i in range(len(self.capital_allocation_levels) - 1, -1, -1): 
            if _current_drawdown > self.drawdown_levels[i]: #Drawdown
                if self.drawdown_flag != i:
                    self.issue_notification_level_update(_date, 'Drawdown', _current_drawdown, self.drawdown_levels[i])
                if self.current_allocation_level > self.capital_allocation_levels[i]:
                    self.issue_notification_capital_update(_date, 'Drawdown', self.capital_allocation_levels[i])
                    self.current_allocation_level = self.capital_allocation_levels[i]
                self.drawdown_flag = i
                break
        if _current_drawdown < self.drawdown_levels[0]:
            self.drawdown_flag = -1

        if _current_loss > self.maxloss: #Maxloss
            if self.maxloss_flag != 0:
                self.issue_notification_level_update(_date, 'Maxloss', _current_loss, self.maxloss)
            if self.current_allocation_level > self.capital_allocation_levels[-1]:
                self.issue_notification_capital_update(_date, 'Maxloss', self.capital_allocation_levels[-1])
            self.maxloss_flag = 0
            self.current_allocation_level = self.capital_allocation_levels[-1]
        else:
            self.maxloss_flag = -1

        # If we reach the max trading cost then stop trading and liquidate the portfolio
        if _current_year_trading_cost > self.max_trading_cost: #MaxTradingCost
            if self.max_trading_cost_flag != 0:
                self.issue_notification_level_update(_date, 'Trading Cost', _current_year_trading_cost, self.max_trading_cost)
            if self.current_allocation_level > 0.0:
                self.issue_notification_capital_update(_date, 'Trading Cost', self.capital_allocation_levels[-1])
            self.max_trading_cost_flag = 0
            self.current_allocation_level = self.capital_allocation_levels[-1]

        # REALLOCATION
        # If not fully invested, check the paper returns and reallocate the capital accordingly
        # print 'current allocation level %f'%self.current_allocation_level
        if self.current_allocation_level < 100.0 and self.max_trading_cost_flag == -1: # If not fully allocated and did not reach max trading cost earlier
            _paper_returns = self.simple_performance_tracker.compute_paper_returns(self.return_history)
            #print 'paper_returns : %f'%_paper_returns
            for i in range(0,len(self.reallocation_returns)): # If we have had good enough returns in the past and current allocation is less than desired
                if _paper_returns >= self.reallocation_returns[i]:
                    if i == 0:
                        _desired_allocation = 100.0
                    else:
                        _desired_allocation = self.capital_allocation_levels[i-1]
                    if self.current_allocation_level < _desired_allocation:
                        self.current_allocation_level = _desired_allocation
                        self.issue_notification_capital_update(_date, 'Reallocated', _desired_allocation)
                    break #start at the highest level. Hence we break.
        return self.current_allocation_level