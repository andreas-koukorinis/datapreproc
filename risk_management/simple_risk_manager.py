from risk_manager_algorithm import RiskManagerAlgo

class SimpleRiskManager(RiskManagerAlgo):
    def init(self, _config):
        _capital_allocation_levels = _config.get('RiskManagement', 'capital_allocation_levels').split(',')
        self.capital_allocation_levels = sorted([float(x) for x in _capital_allocation_levels])
        self.current_allocation_level = 100.0 # Fully allocated initially

    def get_current_risk_level(self, date):
        if date.year == self.performance_tracker.current_year_trading_cost[0]:
            _current_year_trading_cost = self.performance_tracker.current_year_trading_cost[1]
        else:
            _current_year_trading_cost = 0.0 #TODO change to transaction cost from simple_performance_tracker
        if self.current_allocation_level == 0: # If we are not trading
            _current_loss = self.simple_performance_tracker.current_loss
        else:
            _current_loss = self.performance_tracker.current_loss
        _current_drawdown = self.simple_performance_tracker.current_drawdown
        if self.current_allocation_level != 0: # If we are still trading
            for i in range(len(self.capital_allocation_levels) - 1, -1, -1):
                if _current_loss > self.stoploss_levels[i]:
                    self.issue_notification_level_update(date, 'StopLoss', _current_loss, self.stoploss_levels[i])
                    self.issue_notification_capital_update(date, 'StopLoss', self.capital_allocation_levels[i])
                    self.current_allocation_level = self.capital_allocation_levels[i]
                if _current_drawdown > self.drawdown_levels[i]:
                    self.issue_notification_level_update(date, 'Drawdown', _current_drawdown, self.drawdown_levels[i])
                    self.issue_notification_capital_update(date, 'Drawdown', self.capital_allocation_levels[i])
                    self.current_allocation_level = self.capital_allocation_levels[i]
            if _current_loss > self.maxloss:
                self.current_allocation_level = self.capital_allocation_levels[-1]
                self.issue_notification_level_update(date, 'Maxloss', _current_loss, self.maxloss)
                self.issue_notification_capital_update(date, 'Maxloss', self.capital_allocation_levels[-1])
            if _current_year_trading_cost > self.max_trading_cost:
                self.current_allocation_level = self.capital_allocation_levels[-1]
                self.issue_notification_level_update(date, 'Trading Cost', _current_year_trading_cost, self.max_trading_cost)
                self.issue_notification_capital_update(date, 'Trading Cost', self.capital_allocation_levels[-1])
        else:
            
