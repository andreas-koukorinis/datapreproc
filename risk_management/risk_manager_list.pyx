# cython: profile=True
def is_valid_risk_manager_name ( _risk_manager_name ) :
    _risk_manager_name_list = ['SimpleRiskManager', 'RiskManagerPaper', 'RiskManagerVar']
    _retval = False
    if _risk_manager_name in _risk_manager_name_list :
        _retval = True
    else :
        print ("RiskManager_name %s not valid" %(_risk_manager_name))
    return ( _retval )

def get_module_name_from_risk_manager_name ( risk_manager_name ) :
    def _simple_risk_manager():
        return ( "simple_risk_manager" )
    def _risk_manager_paper():
        return ("risk_manager_paper" )
    def _risk_manager_var():
        return ( "risk_manager_var" )

    options = { 'SimpleRiskManager' : _simple_risk_manager,
                'RiskManagerPaper' : _risk_manager_paper,
                'RiskManagerVar' : _risk_manager_var
              }

    if is_valid_risk_manager_name ( risk_manager_name ):
        return ( options[risk_manager_name]() )
