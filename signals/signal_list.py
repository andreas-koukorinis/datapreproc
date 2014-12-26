def is_valid_signal_name(_signal_name):
    _signal_name_list = ['TargetRiskRP', 'TargetRiskEqualRiskContribution', 'TargetRiskMaxSharpeHistCorr','MeanVarianceOptimization']
    _retval = False
    if _signal_name in _signal_name_list:
        _retval = True
    else :
        print "Signalname %s not valid" % _signal_name
    return _retval

def get_module_name_from_signal_name(signal_name):
    def _target_risk_rp():
        return "target_risk_rp"
    def _target_risk_equal_risk_contribution():
        return ( "target_risk_equal_risk_contribution" )
    def _target_risk_max_sharpe_hist_corr():
        return ( "target_risk_max_sharpe_hist_corr" )
    def _mvo():
        return ( "mvo" )

    options = { 'TargetRiskRP' : _target_risk_rp,
                'TargetRiskEqualRiskContribution' : _target_risk_equal_risk_contribution,
                'TargetRiskMaxSharpeHistCorr' : _target_risk_max_sharpe_hist_corr,
                'MeanVarianceOptimization' : _mvo
              }
    if is_valid_signal_name(signal_name):
        return options[signal_name]()
