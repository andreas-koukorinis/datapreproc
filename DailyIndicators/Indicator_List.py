
def is_valid_daily_indicator ( _indicator_module_name ) :
    _indicator_module_names_list = ['DailyLogReturns', 'StdDev', 'Trend','DailyPrice','AverageStdDev','MovingAverage','MovingAverage1','CorrelationLogReturns']
    _retval = False;
    if _indicator_module_name in _indicator_module_names_list :
        _retval = True;
    return (_retval)

