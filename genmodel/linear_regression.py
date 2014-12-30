#!/usr/bin/env python
import sys
import ConfigParser
from numpy import *
from sklearn import linear_model
from utils.process_data import load_data_for_regression

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

    # Split the data into training/testing sets
    n = X.shape[0]
    n1 = int(n*(train_percent/100.0))
    X_train = X[0:n1,:]
    X_test = X[n1+1:n,:]

    # Split the targets into training/testing sets
    Y_train = Y[0:n1]
    Y_test = Y[n1+1:n]

    # Create linear regression object
    regr = linear_model.LinearRegression(fit_intercept=False)

    # Train the model using the training set
    regr.fit(X_train,Y_train)

    print 'Regression: %s ~ %s'%(Y_label,' + '.join(X_labels))

    # The coefficients
    s = 'Coefficients:  '
    for i in xrange(0,len(regr.coef_)):
        s = s + X_labels[i]+' : '+ '%0.10f '%regr.coef_[i]+ '  '
    print s

    # The mean square error
    print ("MSE: %0.10f" %mean((regr.predict(X_test) - Y_test) ** 2))
    # Explained variance score: 1 is perfect prediction
    print ('Variance score: %0.10f' % regr.score(X_test, Y_test))

__main__()
