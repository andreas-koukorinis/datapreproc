def is_valid_execlogic_name ( _execlogic_name ) :
    _execlogic_name_list = ['SimpleExecLogic']
    _retval = False
    if _execlogic_name in _execlogic_name_list :
        _retval = True
    else :
        print ("Execlogic_name %s not valid" %(_execlogic_name))
    return ( _retval )

def get_module_name_from_execlogic_name ( execlogic_name ) :
    def _simple_execlogic():
        return ( "simple_execlogic" )

    options = { 'SimpleExecLogic' : _simple_execlogic
              }

    if is_valid_execlogic_name ( execlogic_name ):
        return ( options[execlogic_name]() )
