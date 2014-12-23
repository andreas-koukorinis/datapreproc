def is_valid_strategy_name ( _strategy_name ) :
    _strategy_name_list = ['CTAMomentum', 'CWAS', 'EqualWeight', 'Momentumv1', 'UnleveredDMF', 'UnleveredRP', 'AggregatorCWAS', 'AggregatorIV','MVO']
    _retval = False
    if _strategy_name in _strategy_name_list :
        _retval = True
    else :
        print ( "Strategyname %s not valid" %(_strategy_name) )
    return ( _retval )

def get_module_name_from_strategy_name ( strategy_name ) :
    def _c_t_a_momentum():
        return ( "CTAMomentum" )
    def _cwas():
        return ( "CWAS" )
    def _equal_weight():
        return ( "EqualWeight" )
    def _momentumv1():
        return ( "Momentumv1" )
    def _unlevered_d_m_f():
        return ( "UnleveredDMF" )
    def _unlevered_r_p():
        return ( "UnleveredRP" )
    def _aggregator_cwas():
        return ( "aggregator_cwas" )
    def _aggregator_iv():
        return ( "aggregator_iv" )
    def _mvo():
        return ( "mvo" )

    options = { 'CTAMomentum' : _c_t_a_momentum,
                'CWAS' : _cwas,
                'EqualWeight' : _equal_weight,
                'Momentumv1' : _momentumv1,
                'UnleveredDMF' : _unlevered_d_m_f,
                'UnleveredRP' : _unlevered_r_p,
                'AggregatorCWAS' : _aggregator_cwas,
                'AggregatorIV' : _aggregator_iv,
                'MVO': _mvo
                }

    if is_valid_strategy_name ( strategy_name ):
        return ( options[strategy_name]() )
