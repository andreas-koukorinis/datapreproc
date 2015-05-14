#!/bin/sh
source /apps/pythonenv/py2.7/bin/activate
#cd /home/cvdev/stratdev/data_cleaning/csi_scripts/
cd /home/cvdev/datapreproc/data_cleaning/quandl_scripts/
echo "DB DUMP QUANDL YIELD RATES STATUS:\n" >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
python daily_update_quandl.py `date +%Y%m%d -d "yesterday"` >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`