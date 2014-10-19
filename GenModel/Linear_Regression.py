#!/usr/bin/env python
import sys
import pickle
import ConfigParser
from numpy import *
from sklearn import linear_model
from ProcessData import load_data_for_regression

def __main__():

    config_file = sys.argv[1]
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file,'r'))

    # Read config file
    target = config.get( 'Data', 'target' )   
    inputs = config.get( 'Data', 'inputs' ).strip().split(",")
    train_percent = config.getfloat('Parameters','train_percent') # The percentage of data to be used for training

    # Y is a 1D numpy array of targets
    # X is a kD numpy array of inputs(k is the number of inputs per target)
    (Y,X) = load_data_for_regression(target,inputs)
    print Y
    print X
    # Split the data into training/testing sets
    n = X.shape[0]
    n1 = int(n*(train_percent/100.0))
    X_train = X[0:n1,:]
    X_test = X[n1+1:n,:]

    # Split the targets into training/testing sets
    Y_train = Y[0:n1]
    Y_test = Y[n1+1:n]

    # Create linear regression object
    regr = linear_model.LinearRegression()

    # Train the model using the training sets
    regr.fit(X_train,Y_train)

    # The coefficients
    print 'Coefficients: %0.10f %0.10f\n'%(regr.coef_[0],regr.coef_[1])
    # The mean square error
    print ("Residual sum of squares: %.2f" %mean((regr.predict(X_test) - Y_test) ** 2))
    # Explained variance score: 1 is perfect prediction
    print ('Variance score: %.2f' % regr.score(X_test, Y_test))

__main__()
