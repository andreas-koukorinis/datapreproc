import os
import sys
import cPickle
import subprocess
import pandas

def __main__():
    signal_config_paths = sys.argv[1:]
    all_dates = []
    df_columns = ['date']
    out_file = ''
    dfs = []
    for signal_config_path in signal_config_paths:
        proc = subprocess.Popen(['python', '-W', 'ignore', 'run_simulator.py', signal_config_path ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        proc.communicate()
        signal_config_name = os.path.splitext(os.path.basename(signal_config_path))[0]
        out_file += signal_config_name + '_'
        return_file_path = os.path.expanduser('~') + '/logs/' + signal_config_name + '/returns.txt'
        signal_name = signal_config_name.split('_')[1]
        df_columns.append(signal_name)
        return_file_handle = open(return_file_path, 'rb')
        dates = cPickle.load(return_file_handle)
        returns = cPickle.load(return_file_handle)
        all_dates.append(dates)
        df_ = pandas.DataFrame(columns=['date', 'return'])
        df_['date'] = dates
        df_.set_index(['date'], inplace = True)
        df_['return'] = returns
        dfs.append(df_)

    df = pandas.DataFrame(columns=df_columns)
    dates_union = sorted(list(set().union(*all_dates)))
    df['date'] = dates_union
    df.set_index(['date'], inplace = True)
    df.sort_index(inplace=True)
    for i in range(len(dfs)): 
        df[df_columns[i+1]] = 0.0
        for index,value in dfs[i].iterrows():
            df.loc[index, df_columns[i+1]] = value['return']

    out_path = os.path.expanduser('~') + '/logs/' + out_file + '.csv'
    df.to_csv(out_path)

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        sys.exit('specify atleast 1 signal config')
    __main__()
