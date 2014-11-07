def is_valid_strategy_name ( _strategy_name ) :
    _strategy_name_list = ['CWAS', 'EqualWeight', 'TargetRiskRP', 'UnleveredRP', 'UnleveredDMF']
    _retval = False
    if _strategy_name in _strategy_name_list :
        _retval = True
    else :
        print ( "Strategyname %s not valid" %(_strategy_name) )
    return ( _retval )
