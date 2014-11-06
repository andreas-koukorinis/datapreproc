from numpy import *
import pandas as pd

def load_data_for_regression(target,inputs):
    X = []
    X_labels = []
    inputs = inputs.strip().split(' ') # Each inp corresponds to one file
    for item in inputs:
        inp = item.strip().split(',') 
        file_name = inp[0]
        df = pd.read_csv(file_name) # Load csv as dataframe
        column_names = inp[1:]
        for column_name in column_names:
            column_data = df[column_name]
            X.append(df[column_name].values)
            X_labels.append(column_name)
    X = array(X).T

    target = target.strip().split(',')
    file_name = target[0]
    if(file_name=='ones'): # If target_filename ='ones' then generate a list of ones
        Y = array([1.0]*X.shape[0])
        Y_label = 'ones'
    else: # Else load target from file
        df = pd.read_csv(file_name)
        column_name = target[1]
        Y = df[column_name].values 
        Y_label = column_name
    return (Y,X,Y_label,X_labels)
    
'''
def load_data(target,inputs):
    all_dates = []
    all_series = []
    all_data_files = [target]
    for inp in inputs:
        all_data_files.append(inp)
    for data_file in all_data_files:    
        dates_series = pickle.load(open(data_file, 'r'))
        dates = [item[0] for item in dates_series]
        series = array([item[1] for item in dates_series]).astype(float)
        all_dates.append(dates)
        all_series.append(series)
    filtered_series = filter_series(all_dates,all_series)
    return(filtered_series[:,0],filtered_series[:,1:])        

def filter_series(all_dates,all_series):
    if(len(all_series)==1):
        return (array(all_series).T).astype(float)
    intersected_dates = list(set(all_dates[0]).intersection(*all_dates))
    intersected_dates.sort()
    filtered_series = []
    for i in xrange(0,len(all_series)):
        Indexes = sort(searchsorted(all_dates[i],intersected_dates))
        filtered_series.append(all_series[i][Indexes])        
    filtered_series = (array(filtered_series).T).astype(float)
    return filtered_series
'''

#print load_data_for_regression('ones','../Data/print_indicators_config_PrintIndicators.csv,DailyLogReturns.fES1,DailyLogReturns.fTY1')
