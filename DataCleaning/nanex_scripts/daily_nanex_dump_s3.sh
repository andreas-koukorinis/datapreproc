#!/bin/sh

cd /home/cvdev/data/nanex/
s3cmd put `date +%Y%m%d -d "yesterday"`.YC.nxc s3://cvquantdata/nanex/rawdata/ >> log_`date +%Y%m%d -d "yesterday"`
#rm `date +%Y%m%d -d "yesterday"`.YC.nxc
