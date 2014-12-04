import os
import sys
import pickle
import datetime
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import pandas as pd
from mpldatacursor import datacursor
import numpy as np

def plot_returns(_returns_files):
    _out_file = 'logs/pngs/'
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
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.13),fancybox=True, ncol=4,prop={'size':6})
    plt.xlabel("Time")
    plt.ylabel("Cumulative returns")
    plt.xticks(rotation=35)
    plt.savefig(_out_file+'.png') #bbox_inches='tight'

def plot_series(_files):
    _out_file = 'logs/pngs/'
    dateparse = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date()
    ax_defined = False
    for _file in _files:
        _out_file += _file.split('/')[-2] + '_' + _file.split('/')[-1].split('.')[0] + '_'
        df = pd.read_csv(_file,parse_dates =['date'],header=0,date_parser=dateparse)
        if len(_files) > 1:
            new_columns = []
            for column in df.columns:
                if column != 'date':
                    new_columns.append( column + '_' + _file.split('/')[-2])
                else:
                    new_columns.append( column )
            df.columns = new_columns   
        df = df.set_index('date')
        if ax_defined:
            df.plot(ax=ax)
        else:
            ax = df.plot()
            ax_defined = True
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.13),fancybox=True,  ncol=4,prop={'size':6})
    plt.xlabel("Time")
    plt.ylabel(_files[0].split('/')[-1].split('.')[0])
    plt.xticks(rotation=35)
    plt.savefig(_out_file+'.png') #bbox_inches='tight'

def plot_series_separately(_files):
    _out_file = 'logs/pngs/'
    dateparse = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date()
    for _file in _files:
        df = pd.read_csv(_file,parse_dates =['date'],header=0,date_parser=dateparse)
        for column in df.columns:
            if column != 'date':
                df1 = df[['date',column]]
                df1 = df1.set_index('date')
                df1.plot()
                plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.13),fancybox=True,  ncol=4,prop={'size':6})
                plt.xlabel("Time")
                plt.ylabel(column)
                plt.xticks(rotation=35)
                plt.savefig('logs/pngs/'+column+'.png')

def plot_returns_separately(_files):
    _out_file = 'logs/pngs/'
    dateparse = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date()
    for _file in _files:
        df = pd.read_csv(_file,parse_dates =['date'],header=0,date_parser=dateparse)
        for column in df.columns:
            if column != 'date':
                df1 = df[['date',column]]
                df1 = df1.set_index('date')
                _returns = df1[column].values
                df1[column] = ( np.exp( np.cumsum( _returns ) ) - 1 )*100.0
                df1.plot()
                plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.13), fancybox=True, ncol=4, prop={'size':6})
                plt.xlabel("Time")
                plt.ylabel(column)
                plt.xticks(rotation=35)
                plt.savefig('logs/pngs/'+column+'.png')

def main():
    if len( sys.argv ) > 1:
        _directory = 'logs/pngs/'
        if not os.path.exists(_directory):
            os.makedirs(_directory)
 
        _type = int(sys.argv[1])
        _files = []
        for i in range(2,len(sys.argv)):
            _files.append(sys.argv[i])
        if _type == 0:
            plot_returns(_files)
        elif _type == 1:
            plot_series(_files)
        elif _type == 2:
            plot_returns_separately(_files)
        elif _type == 3:
            plot_series_separately(_files)

    else:
        sys.exit('python plot_series.py type file1 file2 .. .. filen\nType 0 : returns\nType 1: other series')

if __name__ == '__main__':
    main()
