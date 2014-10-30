class RiskManager():
    def __init__( self, performance_tracker, _config):
        self.performance_tracker = performance_tracker
        self.initial_capital = _config.getfloat( 'Parameters', 'initial_capital' )
        _stoploss_levels = _config.get( 'RiskManagement', 'stoploss_levels' ).split(',')
        self.stoploss_levels = sorted( [ float(x)*self.initial_capital/100.0 for x in _stoploss_levels ] ) # As percentage of initial capital
        _drawdown_levels = _config.get( 'RiskManagement', 'drawdown_levels' ).split(',')
        self.drawdown_levels = sorted( [ float(x) for x in _drawdown_levels ] )
        self.maxloss = _config.getfloat( 'RiskManagement', 'maxloss' )*self.initial_capital/100.0 # As percentage of initial capital
        self.max_trading_cost = _config.getfloat( 'RiskManagement', 'max_trading_cost' )*self.initial_capital/100.0
        self.capital_reduction = _config.getfloat( 'RiskManagement', 'capital_reduction' )

    def check_status( self, dt ):
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
        _current_drawdown = self.performance_tracker.current_max_drawdown
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

    def issue_warning( self, dt, element, max_level, level ):
        print 'On %s : %s level %0.2f exceded. Current level: %0.2f'%( dt, element, max_level, level )

    def notify_stop_trading( self, dt, element, max_level, level ):
        print 'On %s STOPPED TRADING: %s level %0.2f exceded. Current level: %0.2f'%( dt, element, max_level, level )
    
    def notify_reduce_capital( self, dt, element, capital_reduction, max_level, level ):
        print 'On %s REDUCED CAPITAL BY %0.2f%%: %s level %0.2f exceded. Current level: %0.2f'%( dt, capital_reduction, element, max_level, level )                   
