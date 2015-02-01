import os
import sys
import commands
import datetime
import operator

def common_contracts(product, start_date, end_date):
    # Directory for the product
    dir1 = '/apps/data/csi/history_part1/' + product + '/'
    dir2 = '/apps/data/csi/history_part2/' + product + '/'
    dir3 = '/apps/data/csi/historical_futures1/' + product + '/'
    dir4 = '/apps/data/csi/historical_futures2/' + product + '/'
    if os.path.isdir(dir1):
        in_dir = dir1
    elif os.path.isdir(dir2):
        in_dir = dir2
    elif os.path.isdir(dir3):
        in_dir = dir3
    elif os.path.isdir(dir4):
        in_dir = dir4
    else:
        sys.exit('Not Found')
    _date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    delta = datetime.timedelta(days=1)
    counts = {}
    while _date <= end_date:
        cmd = "cd %s;grep %s *.csv | sed 's/:/,/' | sort -k7,7 -rg -t, | head -n 1 | awk -F, '{ print $1 }'" % (in_dir, _date.strftime('%Y%m%d'))
        _date += delta
        output = commands.getoutput(cmd)
        if output == '' or 'sort' in output:
            continue
        if output not in counts.keys():
            counts[output] = 1
        else:
            counts[output] = counts[output] + 1
    v1 = sorted(counts.items(), key=operator.itemgetter(1),reverse=True)
    v2 = sorted(v1, key=lambda tup: tup[0][-8:])
    print v2

def __main__():
    if len(sys.argv) > 1:
        product = sys.argv[1]
        if len(sys.argv) > 3:
            start_date = sys.argv[2]
            end_date = sys.argv[3]
        else:
            start_date = '2013-01-01'
            end_date = '2014-10-31'
    else:
        print 'args: product'
        sys.exit(0)
    common_contracts(product, start_date, end_date)

if __name__ == '__main__':
    __main__()
