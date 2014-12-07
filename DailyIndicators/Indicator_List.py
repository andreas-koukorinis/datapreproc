
def is_valid_daily_indicator ( _indicator_module_name ) :
    _indicator_module_names_list = ['DailyLogReturns', 'StdDev', 'Trend','DailyPrice','AverageStdDev','MovingAverage','MovingAverage1','StdDevCrossover','CorrelationLogReturns']
    _retval = False;
    if _daily_indicator_module_name in _indicator_module_names_list :
        _retval = True;
    else :
        print ( "DailyIndicator module name %s not valid" %(_daily_indicator_module_name) )
    return (_retval)

