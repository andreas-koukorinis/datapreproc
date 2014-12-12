import numpy as np

class RiskManagerAlgo():
    def __init__(self, performance_tracker, simple_performance_tracker, _config):
        self.performance_tracker = performance_tracker
        self.simple_performance_tracker = simple_performance_tracker
        self.weights = np.zeros()
        _stoploss_levels = _config.get('RiskManagement', 'stoploss_levels').split(',')
        self.stoploss_levels = sorted([float(x) for x in _stoploss_levels]) # As percentage of initial capital
        _drawdown_levels = _config.get('RiskManagement', 'drawdown_levels').split(',')
        self.drawdown_levels = sorted([float(x) for x in _drawdown_levels])
        self.maxloss = _config.getfloat('RiskManagement', 'maxloss') # As percentage of initial capital
        self.max_trading_cost = self.performance_tracker.initial_capital*_config.getfloat('RiskManagement', 'max_trading_cost')/100.0
        self.drawdown_flag = -1
        self.stoploss_flag = -1
        self.maxloss_flag = -1
        self.max_trading_cost_flag = -1
        self.init(_config)

    def issue_notification_level_update(self, date, label, current_level, max_level)
        print 'On %s %s level %0.2f exceded. Current level: %0.2f' % (date, label, max_level, current_level)         

    def issue_notification_capital_update(self, date, capital_level):
        print 'On %s CAPITAL ALLOCATION changed to %0.2f%%'
                
