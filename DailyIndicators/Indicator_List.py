
def is_valid_daily_indicator ( _daily_indicator_module_name ) :
    _indicator_module_names_list = ['DailyLogReturns', 'StdDev', 'Trend','DailyPrice','AverageStdDev','MovingAverage','MovingAverage1','StdDevCrossover','CorrelationLogReturns']
    _retval = False;
    if _daily_indicator_module_name in _indicator_module_names_list :
        _retval = True;
    else :
        print ( "DailyIndicator module name %s not valid" %(_daily_indicator_module_name) )
    return (_retval)

def get_module_name_from_indicator_name ( indicator_name ) :
    if is_valid_daily_indicator ( indicator_name ):
        if indicator_name == "AverageDiscretizedTrend":
            return ("AverageDiscretizedTrend")
        if indicator_name == "AverageStdDev":
            return ("AverageStdDev")
        if indicator_name == "CorrelationLogReturns":
            return ("CorrelationLogReturns")
        if indicator_name == "DailyLogReturns":
            return ("DailyLogReturns")
        if indicator_name == "MovingAverage":
            return ("MovingAverage")
        if indicator_name == "StdDevCrossover":
            return ("StdDevCrossover")
        if indicator_name == "StdDev":
            return ("StdDev")
        if indicator_name == "Trend":
            return ("Trend")
        return (indicator_name)
    else:
        print ( STDERR "get_module_name_from_indicator_name called with bad indicator_name %s" %(indicator_name) )
        sys.exit(0)
