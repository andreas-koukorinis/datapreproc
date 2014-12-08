#!/bin/sh

cd /home/cvdev/stratdev/DataCleaning/nanex_scripts/

#fname="20141103.YC.nxc"
var='s3cmd ls s3://cvquantdata/nanex/rawdata/'
var1=`$var$fname`
echo $var1
if [ -n "$var1" ]; then
    mail -s "Nanex tape successfully uploaded on `date +%Y%m%d -d "yesterday"`" "sanchit.gupta@tworoads.co.in" <<< ""
else
    mail -s "Alert! Nanex tape not uploaded on `date +%Y%m%d -d "yesterday"`" "sanchit.gupta@tworoads.co.in" <<< ""
fi
