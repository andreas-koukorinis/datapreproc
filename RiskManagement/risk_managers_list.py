def is_valid_risk_manager_name ( _risk_manager_name ) :
    _risk_manager_name_list = ['SimpleRiskManager']
    _retval = False
    if _risk_manager_name in _risk_manager_name_list :
        _retval = True
    else :
        print ("RiskManager_name %s not valid" %(_risk_manager_name))
    return ( _retval )

def get_module_name_from_risk_manager_name ( risk_manager_name ) :
    def _simple_risk_manager():
        return ( "simple_risk_manager" )

    options = { 'SimpleRiskManager' : _simple_risk_manager
              }

    if is_valid_risk_manager_name ( risk_manager_name ):
        return ( options[risk_manager_name]() )
