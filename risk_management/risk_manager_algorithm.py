

class RiskManagerAlgo():
    def __init__(self, performance_tracker, simple_performance_tracker, _config):
        self.performance_tracker = performance_tracker
        self.simple_performance_tracker = simple_performance_tracker
        self.drawdown_flag = -1
        self.stoploss_flag = -1
        self.maxloss_flag = -1
        self.max_trading_cost_flag = -1
        self.init(_config)

    def issue_notification_level_update(self, date, label, current_level, max_level):
        print 'On %s %s level %0.2f exceded. Current level: %0.2f' % (date, label, max_level, current_level)         

    def issue_notification_capital_update(self, date, label, capital_level):
        print 'On %s CAPITAL ALLOCATION changed to %0.2f%% (%s)' % (date, capital_level, label)
                
