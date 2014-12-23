def is_valid_signal_name(_signal_name):
    _signal_name_list = ['TargetRiskRP', 'TargetRiskVolatilityParity', 'TargetRiskEqualRiskContribution', 'TargetRiskMaxSharpeHistCorr', 'UnleveredDMF']
    _retval = False
    if _signal_name in _signal_name_list:
        _retval = True
    else :
        print "Signalname %s not valid" % _signal_name
    return _retval

def get_module_name_from_signal_name(signal_name):
    def _target_risk_rp():
        return "target_risk_rp"
    def _target_risk_volatility_parity():
        return "target_risk_volatility_parity"
    def _target_risk_equal_risk_contribution():
        return ( "target_risk_equal_risk_contribution" )
    def _target_risk_max_sharpe_hist_corr():
        return ( "target_risk_max_sharpe_hist_corr" )
    def _unlevered_dmf():
        return ( "unlevered_dmf" )

    options = { 'TargetRiskRP' : _target_risk_rp,
                'TargetRiskVolatilityParity' : _target_risk_volatility_parity,
                'TargetRiskEqualRiskContribution' : _target_risk_equal_risk_contribution,
                'TargetRiskMaxSharpeHistCorr' : _target_risk_max_sharpe_hist_corr,
                'UnleveredDMF' : _unlevered_dmf
              }
    if is_valid_signal_name(signal_name):
        return options[signal_name]()
