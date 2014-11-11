#!/bin/sh

cd /home/cvdev/stratdev/DataCleaning/csi_scripts/

for f in canada.`date +%Y%m%d -d "yesterday"`.gz f-indices.`date +%Y%m%d -d "yesterday"`.gz funds.`date +%Y%m%d -d "yesterday"`.gz futures.`date +%Y%m%d -d "yesterday"`.gz indices.`date +%Y%m%d -d "yesterday"`.gz uk-stocks.`date +%Y%m%d -d "yesterday"`.gz us-stocks.`date +%Y%m%d -d "yesterday"`.gz
do
 if [ ! -f $f ]; then
    echo "File $f not found!" >> log_check_`date +%Y%m%d -d "yesterday"`
    mail -s "File $f not found om ftp on `date +%Y%m%d -d "yesterday"`" "sanchit.gupta@tworoads.co.in" <<< ""
    continue
 fi
 echo "Processing $f" >> log_check_`date +%Y%m%d -d "yesterday"`
 s3cmd get s3://cvquantdata/csi/rawdata/$f $f-1
 sum1=$(md5sum $f | awk '{print $1}')
 sum2=$(md5sum $f-1 | awk '{print $1}')
 if [ "$sum1" == "$sum2" ]
 then 
  echo "File $f uploaded successfully" >> log_check_`date +%Y%m%d -d "yesterday"`
  rm $f $f-1
 else
  mail -s "File $f not uploaded to bucket" "sanchit.gupta@tworoads.co.in" <<< ""
 fi
done
mail -s "Check ended on `date +%Y%m%d -d "yesterday"`" "sanchit.gupta@tworoads.co.in" <<< ""
