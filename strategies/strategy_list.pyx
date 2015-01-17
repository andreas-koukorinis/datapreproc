# cython: profile=True
def is_valid_strategy_name ( _strategy_name ) :
    _strategy_name_list = ['AggregatorCWAS', 'AggregatorIV', 'AggregatorSharpe', 'AggregatorBELReturn', 'AggregatorBELSharpe']
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
    def _aggregator_sharpe():
        return ( "aggregator_sharpe" )
    def _aggregator_bel_return():
        return ( "aggregator_bel_return" )
    def _aggregator_bel_sharpe():
        return ( "aggregator_bel_sharpe" )

    options = { 'AggregatorCWAS' : _aggregator_cwas,
                'AggregatorIV' : _aggregator_iv,
                'AggregatorSharpe' : _aggregator_sharpe,
                'AggregatorBELReturn' : _aggregator_bel_return,
                'AggregatorBELSharpe' : _aggregator_bel_sharpe
                }

    if is_valid_strategy_name ( strategy_name ):
        return ( options[strategy_name]() )
