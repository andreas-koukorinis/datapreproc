def is_valid_strategy_name ( _strategy_name ) :
    _strategy_name_list = ['CTAMomentum', 'CWAS', 'EqualWeight', 'Momentumv1', 'TargetRiskRP', 'TargetRiskRPv1', 'UnleveredDMF', 'UnleveredRP', 'TargetRiskEqualRiskContribution', 'TargetRiskMaxSharpeHistCorr' ]
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
        return ( "TargetRiskRP" )
    def _target_risk_rp_v1():
        return ( "TargetRiskRPv1" )
    def _unlevered_d_m_f():
        return ( "UnleveredDMF" )
    def _unlevered_r_p():
        return ( "UnleveredRP" )
    def _target_risk_equal_risk_contribution():
        return ( "target_risk_equal_risk_contribution" )
    def _target_risk_max_sharpe_hist_corr():
        return ( "target_risk_max_sharpe_hist_corr" )

    options = { 'CTAMomentum' : _c_t_a_momentum,
                'CWAS' : _cwas,
                'EqualWeight' : _equal_weight,
                'Momentumv1' : _momentumv1,
                'TargetRiskRP' : _target_risk_rp,
                'TargetRiskRPv1' : _target_risk_rp_v1,
                'UnleveredDMF' : _unlevered_d_m_f,
                'UnleveredRP' : _unlevered_r_p,
                'TargetRiskEqualRiskContribution' : _target_risk_equal_risk_contribution,
                'TargetRiskMaxSharpeHistCorr' : _target_risk_max_sharpe_hist_corr
                }
    if is_valid_strategy_name ( strategy_name ):
        return ( options[strategy_name]() )
