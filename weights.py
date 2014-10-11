from numpy import sum,array,sign,sqrt,ones,absolute
from getStdev import get_simple_stdev,get_exponential_stdev,get_annualized_stdev

def initialize_weights ( _num_products ) :
    # initially set weights to 1/n
    _weights = ones(_num_products)
    _weights = _weights / sum ( absolute ( _weights ) )
    return ( _weights )

# Returns Weights for unlevered trend following RP portfolio[Demystified]
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# day : the day number on which the weights are to be calculated
# weightfunc_args[1] = lookback_trend : how many days in the past are considered for deciding the trend
# weightfunc_args[0] = lookback risk : number of days to be used to calculate the risk associated with the instrument
####weightfunc_args -> list(lookback_risk,lookback_trend)
# To have no forward looking bias, we should make sure that we are not using any data of 'day' or any date >= 'day'
def setWeightsUnleveredDemystified(data,day,weightfunc_args):
    if len ( weightfunc_args ) < 2 :
        return ( initialize_weights ( data.shape[1] ) )
    lookback_risk=weightfunc_args[0]
    lookback_trend=weightfunc_args[1]
    _first_day_index_for_risk = max ( 0, ( day - lookback_risk ) )
    _first_day_index_for_trend = max ( 0, ( day - lookback_trend ) )
    past_ret = array ( data [ _first_day_index_for_risk:day, : ] )
    risk = get_simple_stdev(past_ret)

    # weights = Sign(excess returns)/Risk
    _weights = sign ( sum ( data [ _first_day_index_for_trend:day, : ],axis=0))/risk
    # normalize the weights to ensure unlevered portfolio
    _weights = _weights/sum(absolute(_weights))
    print '\nMoney Allocated:'
    print _weights*100000
    print 'Risk:'
    print risk*sqrt(251)
    return _weights


# Returns Weights for unlevered RP portfolio
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# day : the day number on which the weights are to be calculated
# weightfunc_args[0] = lookback risk : number of days to be used to calculate the risk associated with the instrument
####weightfunc_args -> list(lookback_risk)
# To have no forward looking bias, we should make sure that we are not using any data of 'day' or any date >= 'day'
def setWeightsUnleveredRP(data,day,weightfunc_args):
    if len ( weightfunc_args ) < 1 :
        return ( initialize_weights ( data.shape[1] ) )
    lookback_risk=weightfunc_args[0]
    past_ret = array(data[(day-lookback_risk):(day),:])
    risk = get_simple_stdev(past_ret)
    _weights = 1/risk # weights = 1/Risk
    _weights = _weights/sum(absolute(_weights))
    print '\nMoney Allocated:'
    print _weights*100000
    print 'Risk:'
    print risk*sqrt(251) # normalize the weights to ensure unlevered portfolio
    return _weights


# Returns Weights for unlevered RP portfolio
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# day : the day number on which the weights are to be calculated
# weightfunc_args[0] = lookback risk : number of days to be used to calculate the risk associated with the instrument
####weightfunc_args -> list(lookback_risk)
# To have no forward looking bias, we should make sure that we are not using any data of 'day' or any date >= 'day'
def setWeightsTargetRiskRP(data,day,weightfunc_args):
    if len ( weightfunc_args ) < 2 :
        return ( initialize_weights ( data.shape[1] ) )
    lookback_risk = weightfunc_args[0]
    _first_day_index_for_risk = max ( 0, ( day - lookback_risk ) )

    target_risk = ( float ( weightfunc_args[1] ) / 100 ) # the target risk is specified as a percentage
    past_ret = array ( data[ _first_day_index_for_risk:day, : ] )
    annualized_risk = get_annualized_stdev(past_ret)
    _weights = ( target_risk / annualized_risk.shape[0] ) / annualized_risk	# weights = target_risk/num_products/annualized_risk
    return ( _weights )
