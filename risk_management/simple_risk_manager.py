from risk_manager_algorithm import RiskManagerAlgo

class SimpleRiskManager(RiskManagerAlgo):
    def init(self, _config):
        pass

    def check_status(self, dt):
        status = { 'reduce_capital' : [ False, self.capital_reduction/100.0 ], 'stop_trading' : False }
        #Stoploss
        _current_loss = self.performance_tracker.current_loss
        if _current_loss > 0:
            if _current_loss > self.stoploss_levels[2]:
                status['stop_trading'] = True
                self.notify_stop_trading( dt, 'Stoploss', self.stoploss_levels[2], _current_loss )
            elif _current_loss > self.stoploss_levels[1]:
                status['reduce_capital'][0] = True
                self.notify_reduce_capital( dt, 'Stoploss', self.capital_reduction, self.stoploss_levels[1], _current_loss ) 
            elif _current_loss > self.stoploss_levels[0]:
                self.issue_warning( dt, 'Stoploss', self.stoploss_levels[0], _current_loss )
        #Drawdown
        _current_drawdown = self.performance_tracker.current_drawdown
        if _current_drawdown > 0:
            if _current_drawdown > self.drawdown_levels[2]:
                status['stop_trading'] = True
                self.notify_stop_trading( dt, 'Drawdown', self.drawdown_levels[2], _current_drawdown )
            elif _current_drawdown > self.drawdown_levels[1]:
                status['reduce_capital'][0] = True
                self.notify_reduce_capital( dt, 'Drawdown', self.capital_reduction, self.drawdown_levels[1], _current_drawdown )
            elif _current_drawdown > self.drawdown_levels[0]:
                self.issue_warning( dt, 'Drawdown', self.drawdown_levels[0], _current_drawdown )
        #Maxloss
        if _current_loss > self.maxloss:
            status['stop_trading'] = True
            self.notify_stop_trading( dt, 'Maxloss', self.maxloss, _current_loss )
        #TradingCost
        _trading_cost = self.performance_tracker.trading_cost
        if _trading_cost > self.max_trading_cost:
            status['stop_trading'] = True
            self.notify_stop_trading( dt, 'MaxTradingCost', self.max_trading_cost, _trading_cost )
        return status
