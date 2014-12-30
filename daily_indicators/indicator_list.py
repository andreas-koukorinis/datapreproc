
def is_valid_daily_indicator ( _daily_indicator_module_name ) :
    _indicator_module_names_list = ['DailyLogReturns', 'StdDev', 'Trend','DailyPrice','AverageStdDev','MovingAverage','MovingAverage1','StdDevCrossover','CorrelationLogReturns','AverageDiscretizedTrend','ExpectedReturns']
    _retval = False;
    if _daily_indicator_module_name in _indicator_module_names_list :
        _retval = True;
    else :
        print ( "DailyIndicator module name %s not valid" %(_daily_indicator_module_name) )
    return (_retval)

def get_module_name_from_indicator_name ( indicator_name ) :
    if is_valid_daily_indicator ( indicator_name ):
        if indicator_name == "AverageDiscretizedTrend":
            return ("average_discretized_trend")
        if indicator_name == "AverageStdDev":
            return ("average_stddev")
        if indicator_name == "CorrelationLogReturns":
            return ("correlation_log_returns")
        if indicator_name == "DailyLogReturns":
            return ("daily_log_returns")
        if indicator_name == "MovingAverage":
            return ("moving_average")
        if indicator_name == "StdDevCrossover":
            return ("stddev_crossover")
        if indicator_name == "StdDev":
            return ("stddev")
        if indicator_name == "Trend":
            return ("trend")
        if indicator_name == "ExpectedReturns":
            return ("expected_returns")
        return (indicator_name)
    else:
        print ( "get_module_name_from_indicator_name called with bad indicator_name %s" %(indicator_name) )
        sys.exit(0)
