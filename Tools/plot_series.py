import os
import sys
import pickle
import datetime
import matplotlib.pyplot as plt
from mpldatacursor import datacursor
import numpy as np

def plot_returns(_returns_files):
    _out_file = '/home/cvdev/stratdev/logs/'
    for _file in _returns_files:
        _out_file += _file.split('/')[-2] + '_'
        with open(_file, 'rb') as f:
            _dates_returns = pickle.load(f)
            _dates_returns = [ list(t) for t in zip(*_dates_returns) ]
            _dates, _returns = np.array(_dates_returns[0]), np.array(_dates_returns[1]).astype(float)   
            _cumulative_percent_returns = ( np.exp( np.cumsum( _returns ) ) - 1 )*100.0
            pt = plt.plot(_dates,_cumulative_percent_returns,label=_file.split('/')[-2])
            #datacursor(pt)
            #plt.show()
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15),fancybox=True, shadow=True, ncol=2)
    plt.xlabel("Time")
    plt.ylabel("Cumulative returns")
    plt.savefig(_out_file+'.png') #bbox_inches='tight'

def plot_series(_files):
    _out_file = '/home/cvdev/stratdev/logs/'
    for _file in _files:
        _out_file += _file.split('/')[-2] + '_' + _file.split('/')[-1].split('.')[0] + '_'
        with open(_file, 'rb') as f:
            _dates_series = pickle.load(f)
            _dates_series = [ list(t) for t in zip(*_dates_series) ]
            _dates, _series = np.array(_dates_series[0]), np.array(_dates_series[1]).astype(float)
            pt = plt.plot( _dates, _series, label=_file.split('/')[-2] )
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15),fancybox=True, shadow=True, ncol=2)
    plt.xlabel("Time")
    plt.ylabel("Leverage")
    plt.savefig(_out_file+'.png') #bbox_inches='tight'

def main():
    if len( sys.argv ) > 1:
        _type = int(sys.argv[1])
        _files = []
        for i in range(2,len(sys.argv)):
            _files.append(sys.argv[i])
        if _type == 0:
            plot_returns(_files)
        else:
            plot_series(_files)
    else:
        sys.exit('python plot_series.py type file1 file2 .. .. filen\nType 0 : returns\nType 1: other series')

if __name__ == '__main__':
    main()
