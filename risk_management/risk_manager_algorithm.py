class RiskManagerAlgo():
    def __init__(self, performance_tracker, simple_performance_tracker, _config):
        self.performance_tracker = performance_tracker
        self.simple_performance_tracker = simple_performance_tracker
        self.initial_capital = _config.getfloat( 'Parameters', 'initial_capital' )
        _stoploss_levels = _config.get( 'RiskManagement', 'stoploss_levels' ).split(',')
        self.stoploss_levels = sorted( [ float(x)*self.initial_capital/100.0 for x in _stoploss_levels ] ) # As percentage of initial capital
        _drawdown_levels = _config.get( 'RiskManagement', 'drawdown_levels' ).split(',')
        self.drawdown_levels = sorted( [ float(x) for x in _drawdown_levels ] )
        self.maxloss = _config.getfloat( 'RiskManagement', 'maxloss' )*self.initial_capital/100.0 # As percentage of initial capital
        self.max_trading_cost = _config.getfloat( 'RiskManagement', 'max_trading_cost' )*self.initial_capital/100.0
        self.capital_reduction = _config.getfloat( 'RiskManagement', 'capital_reduction' )
        self.init(_config)

    def issue_warning( self, dt, element, max_level, level ):
        print 'On %s : %s level %0.2f exceded. Current level: %0.2f'%( dt, element, max_level, level )

    def notify_stop_trading( self, dt, element, max_level, level ):
        print 'On %s STOPPED TRADING: %s level %0.2f exceded. Current level: %0.2f'%( dt, element, max_level, level )
    
    def notify_reduce_capital( self, dt, element, capital_reduction, max_level, level ):
        print 'On %s REDUCED CAPITAL BY %0.2f%%: %s level %0.2f exceded. Current level: %0.2f'%( dt, capital_reduction, element, max_level, level )                   
