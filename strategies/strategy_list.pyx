# cython: profile=True
def is_valid_strategy_name ( _strategy_name ) :
    _strategy_name_list = ['AggregatorCWAS', 'AggregatorIV']
    _retval = False
    if _strategy_name in _strategy_name_list :
        _retval = True
    else :
        print ( "Strategyname %s not valid" %(_strategy_name) )
    return ( _retval )

def get_module_name_from_strategy_name ( strategy_name ) :
    def _aggregator_cwas():
        return ( "aggregator_cwas" )
    def _aggregator_iv():
        return ( "aggregator_iv" )

    options = { 'AggregatorCWAS' : _aggregator_cwas,
                'AggregatorIV' : _aggregator_iv
                }

    if is_valid_strategy_name ( strategy_name ):
        return ( options[strategy_name]() )
