#!/bin/sh

cd /home/cvdev/stratdev/DataCleaning/csi_scripts/
echo "S3 DUMP CSI FILES STATUS:\n" >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
python fetch_ftp.py canada.`date +%Y%m%d -d "yesterday"`.gz f-indices.`date +%Y%m%d -d "yesterday"`.gz funds.`date +%Y%m%d -d "yesterday"`.gz futures.`date +%Y%m%d -d "yesterday"`.gz indices.`date +%Y%m%d -d "yesterday"`.gz uk-stocks.`date +%Y%m%d -d "yesterday"`.gz us-stocks.`date +%Y%m%d -d "yesterday"`.gz >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`

cp canada.`date +%Y%m%d -d "yesterday"`.gz /apps/data/csi/
cp f-indices.`date +%Y%m%d -d "yesterday"`.gz /apps/data/csi/
cp funds.`date +%Y%m%d -d "yesterday"`.gz /apps/data/csi/
cp futures.`date +%Y%m%d -d "yesterday"`.gz /apps/data/csi/
cp indices.`date +%Y%m%d -d "yesterday"`.gz /apps/data/csi/
cp uk-stocks.`date +%Y%m%d -d "yesterday"`.gz /apps/data/csi/
cp us-stocks.`date +%Y%m%d -d "yesterday"`.gz /apps/data/csi/
