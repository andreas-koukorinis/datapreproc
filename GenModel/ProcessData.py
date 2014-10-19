from numpy import *
import pickle

def load_data_for_regression(target,inputs):
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
