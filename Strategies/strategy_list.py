def is_valid_strategy_name ( _strategy_name ) :
    _strategy_name_list = ['CTAMomentum', 'CWAS', 'EqualWeight', 'Momentumv1', 'TargetRiskRP', 'UnleveredDMF', 'UnleveredRP', 'TargetRiskEqualRiskContribution', 'TargetRiskMaxSharpeHistCorr', 'AggregatorCWAS', 'AggregatorIV']
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
    def _target_risk_rp():
        return ( "target_risk_rp" )
    def _unlevered_d_m_f():
        return ( "UnleveredDMF" )
    def _unlevered_r_p():
        return ( "UnleveredRP" )
    def _target_risk_equal_risk_contribution():
        return ( "target_risk_equal_risk_contribution" )
    def _target_risk_max_sharpe_hist_corr():
        return ( "target_risk_max_sharpe_hist_corr" )
    def _aggregator_cwas():
        return ( "aggregator_cwas" )
    def _aggregator_iv():
        return ( "aggregator_iv" )

    options = { 'CTAMomentum' : _c_t_a_momentum,
                'CWAS' : _cwas,
                'EqualWeight' : _equal_weight,
                'Momentumv1' : _momentumv1,
                'TargetRiskRP' : _target_risk_rp,
                'UnleveredDMF' : _unlevered_d_m_f,
                'UnleveredRP' : _unlevered_r_p,
                'TargetRiskEqualRiskContribution' : _target_risk_equal_risk_contribution,
                'TargetRiskMaxSharpeHistCorr' : _target_risk_max_sharpe_hist_corr,
                'AggregatorCWAS' : _aggregator_cwas,
                'AggregatorIV' : _aggregator_iv
                }

    if is_valid_strategy_name ( strategy_name ):
        return ( options[strategy_name]() )
