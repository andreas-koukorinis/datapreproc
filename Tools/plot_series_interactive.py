'''This module plots the interactive graphs(hover and display values)
for multiple return series(function : plot_returns(_files)) or
other series like weights,leverage in a single graph for comparison
The files for return series are expected to be in pickle format i.e. pickle dump of list(date,return)
The files for other series are expected to be in csv format i.e. date,return'''

import os
import sys
import pickle
from datetime import datetime
from pygal import DateY
import pandas as pd
import numpy as np

def plot_returns(_files):
    '''This module plots the interactive graphs(hover and display values)
    for multiple return series in a single graph
    The files for return series are expected to be in pickle format i.e.
    pickle dump of list(date,return)'''
    theme = _files[0].split('/')[-1].split('.')[0] # returns
    _out_file = 'logs/svgs/' + theme + '_'
    datey = DateY(x_label_rotation=-25, dots_size=0.1, \
                        y_title='Cumulative Returns', \
                        x_title='Date', legend_font_size=7,\
                        legend_at_bottom=True)
    datey.x_label_format = "%Y-%m-%d"
    for _file in _files:
        _out_file += _file.split('/')[-2] + '_'
        with open(_file, 'rb') as f:
            _dates_returns = pickle.load(f)
            _dates_returns = [list(t) for t in zip(*_dates_returns)]
            _dates = np.array(_dates_returns[0])
            _returns = np.array(_dates_returns[1]).astype(float)
            _cumulative_percent_returns = (np.exp(np.cumsum(_returns)) - 1)*100.0
            datey.add(_file.split('/')[-2], zip(_dates, _cumulative_percent_returns))
    datey.render_to_file(_out_file + '.svg')

def plot_series(_files):
    '''This function plots the interactive graphs(hover and display values)
    for multiple series in a single graph
    The files for series are expected to be in csv format i.e.
    date,return'''
    theme = _files[0].split('/')[-1].split('.')[0] # leverage, weights etc
    _out_file = 'logs/svgs/' + theme + '_'
    datey = DateY(x_label_rotation=-25, dots_size=0.1, \
                        y_title=theme, x_title='Date', \
                        legend_font_size=7, legend_at_bottom=True)
    datey.x_label_format = "%Y-%m-%d"
    dateparse = lambda x: datetime.strptime(x, '%Y-%m-%d').date()
    for _file in _files:
        _out_file += _file.split('/')[-2] + '_'
        df = pd.read_csv(_file, parse_dates=['date'], \
                         header=0, date_parser=dateparse)
        for column in df.columns:
            if column != 'date':
                label = column + '_' + _file.split('/')[-2]
                datey.add(label, zip(df['date'].values, df[column].values))
    datey.render_to_file(_out_file + '.svg')

def main():
    if len(sys.argv) > 1:
        _directory = 'logs/svgs/'
        if not os.path.exists(_directory):
            os.makedirs(_directory)
        _type = int(sys.argv[1])
        _files = []
        for i in range(2, len(sys.argv)):
            _files.append(sys.argv[i])
        if _type == 0:
            plot_returns(_files)
        else:
            plot_series(_files)
    else:
        sys.exit('python plot_series_interactive.py type file1 file2 ... filen\n type:0 => returns\n type:1 => other series\n')

if __name__ == '__main__':
    main()
