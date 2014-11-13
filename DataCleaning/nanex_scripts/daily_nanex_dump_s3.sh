#!/bin/sh

cd /apps/data/nanex/
echo "S3 DUMP NANEX TAPE STATUS:\n" >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
s3cmd put `date +%Y%m%d -d "yesterday"`.YC.nxc s3://cvquantdata/nanex/rawdata/ >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
