#!/bin/sh

cd /home/cvdev/stratdev/data_cleaning/csi_scripts/

echo "S3 DUMP CSI FILES CHECKSUM VERIFY STATUS:\n" >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`

for f in canada.`date +%Y%m%d -d "yesterday"`.gz f-indices.`date +%Y%m%d -d "yesterday"`.gz funds.`date +%Y%m%d -d "yesterday"`.gz futures.`date +%Y%m%d -d "yesterday"`.gz indices.`date +%Y%m%d -d "yesterday"`.gz uk-stocks.`date +%Y%m%d -d "yesterday"`.gz us-stocks.`date +%Y%m%d -d "yesterday"`.gz
do
 if [ ! -f $f ]; then
    echo "ERROR: File $f not found!" >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
    mail -s "File $f not found om ftp on `date +%Y%m%d -d "yesterday"`" "sanchit.gupta@tworoads.co.in" <<< ""
    continue
 fi
 echo "Processing $f" >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
 s3cmd get s3://cvquantdata/csi/rawdata/$f $f-1
 sum1=$(md5sum $f | awk '{print $1}')
 sum2=$(md5sum $f-1 | awk '{print $1}')
 if [ "$sum1" == "$sum2" ]
 then 
  echo "File $f uploaded to s3 successfully.Checksum Matches." >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
  rm $f $f-1
 else
  mail -s "File $f not uploaded to s3" "sanchit.gupta@tworoads.co.in" <<< ""
  echo "ERROR :File $f not uploaded to s3.Checksum does not Match." >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
 fi
done
mail -s "Check ended on `date +%Y%m%d -d "yesterday"`" "sanchit.gupta@tworoads.co.in" <<< ""
echo "Check ended on `date +%Y%m%d -d "yesterday"`" >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
