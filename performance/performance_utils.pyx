import numpy

# Calculates the global maximum drawdown i.e. the maximum drawdown till now
def drawdown(returns):
    if returns.shape[0] < 2:
        return 0.0
    cum_returns = returns.cumsum()
    return -1.0*max(numpy.maximum.accumulate(cum_returns) - cum_returns) # Will return a negative value
