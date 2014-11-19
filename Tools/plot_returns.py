import os
import sys
import pickle
import datetime
import matplotlib.pyplot as plt
from mpldatacursor import datacursor
import numpy as np

def plot_returns(_returns_files):
    _out_file = '../logs/'
    for _file in _returns_files:
        _out_file += _file.split('/')[2] + '_'
        with open(_file, 'rb') as f:
            _dates_returns = pickle.load(f)
            _dates_returns = [ list(t) for t in zip(*_dates_returns) ]
            _dates, _returns = np.array(_dates_returns[0]), np.array(_dates_returns[1]).astype(float)   
            _cumulative_percent_returns = ( np.exp( np.cumsum( _returns ) ) - 1 )*100.0
            pt = plt.plot(_dates,_cumulative_percent_returns,label=_file.split('/')[2])
            datacursor(pt)
            #plt.show()
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.1),fancybox=True, shadow=True, ncol=5)
    plt.xlabel("Time")
    plt.ylabel("Cumulative returns")
    plt.savefig(_out_file+'.svg') #bbox_inches='tight'

def main():
    if len( sys.argv ) > 1:
        _returns_files = []
        for i in range(1,len(sys.argv)):
            _returns_files.append(sys.argv[i])
        plot_returns(_returns_files)
    else:
        sys.exit('python plot_returns.py return_file1 return_file2 .. .. returns_filen')

if __name__ == '__main__':
    main()
