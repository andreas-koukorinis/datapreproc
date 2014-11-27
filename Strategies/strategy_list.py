def is_valid_strategy_name ( _strategy_name ) :
    _strategy_name_list = ['CWAS', 'EqualWeight', 'TargetRiskRP', 'UnleveredRP', 'UnleveredDMF']
    _retval = False
    if _strategy_name in _strategy_name_list :
        _retval = True
    else :
        print ( "Strategyname %s not valid" %(_strategy_name) )
    return ( _retval )

def get_module_name_from_strategy_name ( strategy_name ) :
    def _cwas():
        return ( "cwas" )
    def _equal_weight():
        return ( "equal_weight" )
    def _target_risk_rp():
        return ( "target_risk_rp" )

    options = { 'CWAS' : _cwas,
                'EqualWeight' : _equal_weight,
                'TargetRiskRP' : _target_risk_rp
                }
    if is_valid_strategy_name ( strategy_name ):
        return ( options[strategy_name]() )
