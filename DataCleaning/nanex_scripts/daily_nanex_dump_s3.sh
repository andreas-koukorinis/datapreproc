#!/bin/sh

cd /home/cvdev/stratdev/DataCleaning/nanex_scripts/
s3cmd put `date +%Y%m%d -d "yesterday"`.YC.nxc s3://cvquantdata/nanex/rawdata/ >> log_`date +%Y%m%d -d "yesterday"`
#rm `date +%Y%m%d -d "yesterday"`.YC.nxc
