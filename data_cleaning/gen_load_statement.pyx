# cython: profile=True
#!/usr/bin/env python
import os
import sys
import pandas as pd
from datetime import datetime,date,timedelta

def __main__() :
    if len( sys.argv ) > 1:
        products = []
        table_name = sys.argv[1]
        for i in range(2,len(sys.argv)):
            products.append(sys.argv[i])
        for product in products:
            print "LOAD DATA LOCAL INFILE '/home/cvdev/stratdev/data_cleaning/Data/%s.csv' INTO TABLE %s FIELDS TERMINATED BY ',' IGNORE 1 LINES;"%(product,table_name)
    else:
        print 'python gen_load_statement.py table_name product1 product2 product3... productn'
        sys.exit(0)

if __name__ == '__main__':
    __main__();
