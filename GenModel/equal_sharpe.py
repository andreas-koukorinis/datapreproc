import sys
import ConfigParser
from numpy import *
from sklearn import linear_model
from Utils.ProcessData import load_data_for_regression

# Add a constant to each column of X (n*k array) so that each column of X has the same 'sharpe'
def adjust_data_equal_sharpe(X,sharpe):

    n = X.shape[0]
    S = n*sharpe
    X_new_1 = []
    X_new_2 = []
    for i in xrange(0,X.shape[1]):
        Xi = X[:,i]
        Si = sum(Xi)
        n=Xi.shape[0]
        ks = roots( [(pow(n,2) - S*n) , -2*Si*(S-n) , (pow(Si,2)-S*linalg.norm(Xi))]  )
          
        Xi_new_1 = Xi+ks[0]
        Xi_new_2 = Xi+ks[1]
        X_new_1.append(Xi_new_1)
        X_new_2.append(Xi_new_2)
    X_new_1 = array(X_new_1).T
    X_new_2 = array(X_new_2).T

    return (X_new_1,X_new_2)

def __main__():

    config_file = sys.argv[1]
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file,'r'))

    # Read config file
    target = config.get( 'Data', 'target' )
    inputs = config.get( 'Data', 'inputs' )

    train_percent = config.getfloat('Parameters','train_percent') # The percentage of data to be used for training

    # Y is a 1D numpy array of targets
    # X is a kD numpy array of inputs(k is the number of inputs per target)
    (Y,X,Y_label,X_labels) = load_data_for_regression(target,inputs)

    (X1,X2) = adjust_data_equal_sharpe(X,0.5)

    print linalg.inv(asmatrix(X1).T*asmatrix(X1))*(asmatrix(X1).T*asmatrix(Y).T)
    print linalg.inv(asmatrix(X2).T*asmatrix(X2))*(asmatrix(X2).T*asmatrix(Y).T)

__main__()
