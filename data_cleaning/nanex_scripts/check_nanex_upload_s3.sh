#!/bin/sh

cd /home/cvdev/stratdev/data_cleaning/nanex_scripts/

fname="`date +%Y%m%d -d "yesterday"`.YC.nxc"
var='s3cmd ls s3://cvquantdata/nanex/rawdata/'
var1=`$var$fname`
echo $var1
if [ -n "$var1" ]; then
    mail -s "Nanex tape successfully uploaded on `date +%Y%m%d -d "yesterday"`" "sanchit.gupta@tworoads.co.in" <<< ""
else
    mail -s "Alert! Nanex tape not uploaded on `date +%Y%m%d -d "yesterday"`" "sanchit.gupta@tworoads.co.in" <<< ""
    cd /apps/data/nanex/
    nohup ./NxD_Circulumvite 1 60 2 > nohup.out
fi
